#
# module::rdr.pm
#
# Copyright (c) 2013 Marko Dinic <marko@yu.net>. All rights reserved.
# This program is free software; you can redistribute it and/or
# modify it under the same terms as Perl itself.
#

package module::rdr;

##############################################################################################

use strict;
use warnings;

##############################################################################################

use Socket;
use IO::Socket;
use Config::ContextSensitive qw(:macros);

##############################################################################################

use api::module;

##############################################################################################

our @ISA = qw(api::module);

##############################################################################################

my $CONF_TEMPLATE = SECTION(
    DIRECTIVE('listen_on_address', ARG(CF_INET|CF_ADDR, STORE(TO 'MODULE', KEY { '$SECTION' => { 'listen_address' => '$VALUE' } }), DEFAULT '0.0.0.0')),
    DIRECTIVE('listen_on_port', ARG(CF_PORT, STORE(TO 'MODULE', KEY { '$SECTION' => { 'listen_port' => '$VALUE' } }), DEFAULT '33000')),
    DIRECTIVE('rdr', SECTION_NAME, REQUIRE(
	DIRECTIVE('tag', ARG(CF_INTEGER, STORE(TO 'MODULE', KEY { '$SECTION' => { 'rdr' => { '$NESTED_SECTION' => { 'tag' => '$VALUE' } } } })), SECTION(
	    DIRECTIVE('rdr_field', SKIP, ARG(CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$PARENT_SECTION' => { 'rdr' => { '$SECTION' => { 'field' => { '$ARG[1]' => '$VALUE' } } } } })))
	))
    )),
    DIRECTIVE('rdr_filter', REQUIRE(
	DIRECTIVE('types', ARG(CF_ARRAY, STORE(TO 'MODULE', KEY { '$SECTION' => { 'filter' => { 'rdr' => '$VALUE' } } }))),
    )),
    DIRECTIVE('rdr_output', REQUIRE(
	DIRECTIVE('format', ARG(CF_ARRAY, STORE(TO 'MODULE', KEY { '$SECTION' => { 'record_format' => '$VALUE' } })))
    ))
);

##############################################################################################

# RDR header field index
use constant {
    RDR_TPID			=> 0,
    RDR_LEN			=> 1,
    RDR_SRC_IP_LSB		=> 2, 
    RDR_DST_IP_LSB		=> 3,
    RDR_SRC_PORT		=> 4,
    RDR_DST_PORT		=> 5,
    RDR_FLOW_CONTEXT_ID		=> 6,
    RDR_TAG			=> 7,
    RDR_NUM_FIELDS		=> 8
};

# RDR header field names used in
# output record formatting
my %RDR_HEADER_FIELD = (
    'traffic_processor_id'	=> &RDR_TPID,
    'packet_length'		=> &RDR_LEN,
    'source_ip'			=> &RDR_SRC_IP_LSB,
    'destination_ip'		=> &RDR_DST_IP_LSB,
    'source_port'		=> &RDR_SRC_PORT,
    'destination_port'		=> &RDR_DST_PORT,
    'flow_context_id'		=> &RDR_FLOW_CONTEXT_ID,
    'tag'			=> &RDR_TAG,
    'num_fields'		=> &RDR_NUM_FIELDS
);

# RDR TLV types
use constant {
    RDR_TYPE_INT8		=> 11,
    RDR_TYPE_INT16		=> 12,
    RDR_TYPE_INT32		=> 13,
    RDR_TYPE_UINT8  		=> 14,
    RDR_TYPE_UINT16		=> 15,
    RDR_TYPE_UINT32		=> 16,
    RDR_TYPE_REAL		=> 21,
    RDR_TYPE_BOOLEAN		=> 31,
    RDR_TYPE_STRING		=> 41
};

# RDR TLV type to unpack() format mapping
my %RDR_FIELD_FORMAT = (
    &RDR_TYPE_STRING		=> 'A',
    &RDR_TYPE_BOOLEAN		=> 'C',
    &RDR_TYPE_REAL		=> 'f',
    &RDR_TYPE_INT8		=> 'c',
    &RDR_TYPE_INT16		=> 'n',
    &RDR_TYPE_INT32		=> 'N',
    &RDR_TYPE_UINT8		=> 'C',
    &RDR_TYPE_UINT16		=> 'n',
    &RDR_TYPE_UINT32		=> 'N'
);

# Sizes:
#  - RDR signature length: TPID (1 byte) + ASCII encoded RDR length (4 characters)
#  - RDR header length: 15 bytes
#  - Min RDR length: RDR header length + TLV type (1 byte) + TLV len (4 bytes) + 1 byte of data
#  - Max RDR length: maximum value of a 4 digit number (9999)
#  - Min RDR packet size: RDR signature length + min RDR length
#  - Max RDR packet size: RDR signature length + max RDR length
#  - Receive buffer size: 1000 max-sized RDR packets
use constant SIGNATURE_LEN	=> 1 + 4;
use constant HEADER_LEN		=> 15;
use constant HEADER_NUM_FIELDS	=> 7;
use constant MIN_LEN		=> HEADER_LEN + 1 + 4 + 1;
use constant MAX_LEN		=> 9999;
use constant MIN_PACKET		=> SIGNATURE_LEN + MIN_LEN;
use constant MAX_PACKET		=> SIGNATURE_LEN + MAX_LEN;
use constant BUFFER_SIZE	=> 1000 * MAX_PACKET;

##############################################################################################

sub register() {
    return $CONF_TEMPLATE;
}

sub daemonize($) {
    my $self = shift;

    my $instdata = {};

    return $instdata;
}

sub initialize($$) {
    my ($self, $instdata) = @_;

    # Set global run flag
    $instdata->{'run'} = 1;

    # This is where we keep our established connections
    $instdata->{'connections'} = {};

    # This is our output queue
    $instdata->{'output_queue'} = [];

    # Do the rest of initialization
    return $self->reinitialize($instdata);
}

sub reinitialize($$) {
    my ($self, $instdata) = @_;

    # If listener isn't defined yet or parameters
    # have changed, we need to create it
    unless(defined($instdata->{'listen_address'}) &&
	   $instdata->{'listen_address'} eq $self->{'listen_address'} &&
	   defined($instdata->{'listen_port'}) &&
	   $instdata->{'listen_port'} == $self->{'listen_port'}) {

	# Start listening on specified socket
	my $listener = IO::Socket::INET->new(
				LocalAddr => $self->{'listen_address'},
				LocalPort => $self->{'listen_port'},
				Proto => 'tcp',
				Listen => 1,
				ReuseAddr => 1,
				Blocking => 0
			);

	unless(defined($listener)) {
	    $self->api->logging('LOG_ERR', "RDR collector %s: failed to create listener socket",
					   $self->{'instance'});
	    return undef;
	}

	# If we already have a listener ...
	if(defined($instdata->{'listener'})) {
	    # ... destroy it and it's
	    # established connections
	    $self->cleanup($instdata);
	}
	
	# Keep new listener socket
	$instdata->{'listener'} = $listener;

	# Create I/O event that handles incoming connections by
	# accepting them and assigning them to network receiver
	# callback.
	$self->api->create_io_event('file' => $listener,
				    'op' => 'r',
				    'handler' => \&socket_listener,
				    'args' => [ $self, $instdata ]);

	# Keep current listener's parameters
	$instdata->{'listen_address'} = $self->{'listen_address'};
	$instdata->{'listen_port'} = $self->{'listen_port'};
    }

    # Begin new tag map
    $instdata->{'tag'} = {};

    # Create tag-to-RDR name mapping
    foreach my $rdr_name (keys %{$self->{'rdr'}}) {
	$instdata->{'tag'}{$self->{'rdr'}{$rdr_name}{'tag'}} = $rdr_name;
    }

    # Begin new filter
    $instdata->{'filter'} = {};

    # Prepare RDR tag lookup by translating
    # RDR type names into numeric tags
    foreach my $rdr (@{$self->{'filter'}{'rdr'}}) {
	# RDR name ?
	if(defined($self->{'rdr'}{$rdr})) {
	    # Xlat to numeric form
	    $rdr = $self->{'rdr'}{$rdr}{'tag'};
	}
	# Prepare numeric RDR type (tag) lookup
	$instdata->{'filter'}{'rdr'}{$rdr} = 1;
    }

    # Begin new output format array
    $instdata->{'record_format'} = [];

    # Prepare output format
    foreach my $record_field (@{$self->{'record_format'}}) {
	# Internal variable ?
	if($record_field =~ /^\%([^\%]+)$/) {
	    # Store variable's name
	    push @{$instdata->{'record_format'}}, $1;
	# Field index ?
	} elsif($record_field =~ /^\d+$/) {
	    my $index = int($record_field) - 1;
	    # Store field index
	    push @{$instdata->{'record_format'}}, ($index < 0) ? 0:$index;
	# Then it should be field name
	} else {
	    # Extract RDR type and field name
	    # from one of the following formats:
	    #    RDR_TYPE.RDR_FIELD
	    #    RDR_TYPE->RDR_FIELD
	    #    RDR_TYPE::RDR_FIELD
	    #    RDR_TYPE:RDR_FIELD
	    #    RDR_TYPE|RDR_FIELD
	    #    RDR_TYPE/RDR_FIELD
	    my ($rdr, $field_name) = split(/\.|::|:|\/|\||\->/, $record_field);
	    unless(defined($rdr) && $rdr ne '') {
		$self->api->logging('LOG_ERR', "RDR collector %s: record format is referencing a field without specified RDR type",
					       $self->{'instance'});
		return undef;
	    }
	    unless(defined($self->{'rdr'}{$rdr})) {
		$self->api->logging('LOG_ERR', "RDR collector %s: record format is referencing unknown RDR type %s",
					       $self->{'instance'},
					       $rdr);
		return undef;
	    }
	    # Get field index
	    my $field = int($self->{'rdr'}{$rdr}{'field'}{$field_name});
	    unless(defined($field) && $field > 0) {
		$self->api->logging('LOG_ERR', "RDR collector %s: record format is referencing unknown field %s in RDR type %s",
					       $self->{'instance'},
					       $field_name,
					       $rdr);
		return undef;
	    }
	    my $index = int($field) - 1;
	    # Store field index
	    push @{$instdata->{'record_format'}}, $index;
	}
    }

    # Implement our own output handler
    $instdata->{'runner'} = defined($instdata->{'runner'}) ?
		    $self->api->modify_io_event($self->channel, 'w',
						'args' => [ $self, $instdata ]):
		    $self->api->create_io_event('file' => $self->channel,
						'op' => 'w',
						'handler' => \&queue_runner,
						'args' => [ $self, $instdata ]);

    return $instdata;
}

sub cleanup($$) {
    my ($self, $instdata) = @_;

    foreach my $conn (keys %{$instdata->{'connections'}}) {
	# Remove socket from event monitor
	$self->api->destroy_io_event($conn, 'r');
	# Close socket
	close($conn);
	# Remove buffer parser from event monitor
	$self->api->destroy_loop_event($instdata->{'connections'}{$conn}{'buf_parser'});
	# Remove it from our list
	delete $instdata->{'connections'}{$conn};
    }

    # Remove output queue runner
    $self->api->destroy_loop_event($instdata->{'runner'});
    # Remove listener from event monitor
    $self->api->destroy_io_event($instdata->{'listener'}, 'r');
    # Close listener socket
    close($instdata->{'listener'});
    # Remove it from our list
    delete $instdata->{'listener'};
}

sub abort($$) {
    my ($self, $instdata) = @_;
    # Bring run flag down
    $instdata->{'run'} = 0;
}

##############################################################################################

sub queue_runner($$$) {
    my ($channel, $instance, $instdata) = @_;

    # If we got no records pending ...
    unless(@{$instdata->{'output_queue'}}) {
	# ... delay ourselves for 1 sec
	$instance->api->delay_event($instance->api->get_event($channel, 'w'), 1);
	# ... do some debug logging
	$instance->api->logging('LOG_DEBUG', "RDR collector %s: output queue is empty: queue runner sleeping for 1 second",
					     $instance->{'instance'});
	# ... done for now
	return;
    }

    # Deliver records to the main process
    # (maximum 100 records at a time)
    for(my $i = 0; $instdata->{'run'} && $i < 100; $i++) {
	# Pick records from the top of the queue
	my $record = shift @{$instdata->{'output_queue'}};
	# If no records remain, we are done
	last unless(defined($record));
	# Record must have at least one field
	if($record->[0] ne '') {
	    # Send record to the main process
	    $instance->put_record($record);
	}
    }

    # Explicitly return nothing
    return;
}

##############################################################################################

sub socket_listener($$$) {
    my ($listener, $instance, $instdata) = @_;

    # Accept incoming connection
    my $conn = $listener->accept();

    unless(defined($conn)) {
	$instance->api->logging('LOG_ERR', "RDR collector %s: failed to accept incoming connection",
					   $instance->{'instance'});
	return;
    }

    # Make socket non-blocking
    $instance->api->set_nonblocking($conn);
    # Prepare an empty receiving buffer
    $instdata->{'connections'}{$conn}{'buffer'} = '';
    # Reset buffer data length
    $instdata->{'connections'}{$conn}{'buf_len'} = 0;
    # Reset buffer pointer
    $instdata->{'connections'}{$conn}{'buf_ptr'} = 0;
    # Create loop event that parses buffer contents
    $instdata->{'connections'}{$conn}{'buf_parser'} =
	$instance->api->create_loop_event('handler' => \&header_parser,
					  'args' => [ $instance, $instdata, $conn ]);

    # Create I/O event that reads from network
    $instance->api->create_io_event('file' => $conn,
				    'op' => 'r',
				    'handler' => \&network_receiver,
				    'args' => [ $instance, $instdata ]);

    $instance->api->logging('LOG_INFO', "RDR collector %s: exporter %s connected",
					$instance->{'instance'},
					$conn->peerhost);

    return;
}


sub network_receiver($$$) {
    my ($conn, $instance, $instdata) = @_;

    # How much data in the buffer remains unprocessed ?
    my $remaining = BUFFER_SIZE - $instdata->{'connections'}{$conn}{'buf_ptr'};
    # When remaining unprocessed data is less than
    # a single full-sized RDR packet (9999+1+4) ...
    if($remaining < MAX_PACKET) {
	# ... truncate buffer to release memory
	$instdata->{'connections'}{$conn}{'buffer'} =
		substr($instdata->{'connections'}{$conn}{'buffer'},
		       $instdata->{'connections'}{$conn}{'buf_ptr'},
		       $remaining);
	# ... adjust buffer length counter
	$instdata->{'connections'}{$conn}{'buf_len'} = $remaining;
	# ... and buffer pointer accordingly
	$instdata->{'connections'}{$conn}{'buf_ptr'} = 0;
    }

    # Always attempt to read full size of the receive buffer.
    # Buffer will automatically grow to receive designated
    # number of octets and we already periodically truncate
    # it to release memory by discarding processed content.
    my $num_octets = sysread($conn,
			     $instdata->{'connections'}{$conn}{'buffer'},
			     BUFFER_SIZE,
			     $instdata->{'connections'}{$conn}{'buf_len'});

    # Connection closed (for whatever reason) ?
    unless(defined($num_octets)) {
	$instance->api->logging('LOG_INFO', "RDR collector %s: exporter %s disconnected",
					    $instance->{'instance'},
					    $conn->peerhost);
	# Signal the connection close
	$instdata->{'connections'}{$conn}{'closed'} = 1;
	# Remove socket read event from event monitor
	$instance->api->destroy_io_event($conn, 'r');
	# Close socket
	close($conn);
	return;
    }

    # Add the number of received octets to 
    # the total number of octets in the buffer
    $instdata->{'connections'}{$conn}{'buf_len'} += $num_octets;

    return;
}

sub header_parser($$$$) {
    my ($event, $instance, $instdata, $conn) = @_;

    # Buffer must contain at least minimum
    # size RDR packet in the buffer
    unless($instdata->{'connections'}{$conn}{'buf_len'} -
		$instdata->{'connections'}{$conn}{'buf_ptr'} >= MIN_PACKET) {
	# If connection closed and buffer contains
	# no more complete packets ....
	if($instdata->{'connections'}{$conn}{'closed'}) {
	    # ... remove buffer parser from event monitor
	    $instance->api->destroy_loop_event($event);
	    # ... remove connection from our list
	    delete $instdata->{'connections'}{$conn};
	    # ... and, we are done
	    return;
	}
	# ... delay ourselves for 1 second
	$instance->api->delay_event($event, 1);
	# ... do some debug logging
	$instance->api->logging('LOG_DEBUG', "RDR collector %s: not enough data in the buffer: header parser sleeping for 1 second",
					     $instance->{'instance'});
	# ... done for now
	return;
    }

    # Scan for the beginning of RDR packet
    # (max. 1000 octets at a time)
    for(my $i = 0;
	($i < 1000) &&
	($instdata->{'connections'}{$conn}{'buf_len'} >
	 $instdata->{'connections'}{$conn}{'buf_ptr'});
	$i++, $instdata->{'connections'}{$conn}{'buf_ptr'}++) {

	# Parse RDR signature:
	#  1. TPID - Traffic Processor ID (1 octet)
	#  2. packet length (4 ASCII-encoded digits)
	my ($tpid, $ascii_len) = unpack('x'.$instdata->{'connections'}{$conn}{'buf_ptr'}.' Ca4',
					$instdata->{'connections'}{$conn}{'buffer'});

	# TPID and length must be valid
	next unless(defined($tpid) && $tpid =~ /^\d+$/ &&
		    defined($ascii_len) && $ascii_len =~ /^\d+$/);

	# Convert length string to integer
	my $len = int($ascii_len);

	# Packet length (minus TPID and length fields)
	# must be at least 21 octets:
	#  15 (length of RDR header) +
	#  1 (TLV type) +
	#  4 (TLV length) +
	#  1 (one octet of data)
	# and no more than 9999 octets: length field
	# is ASCII encoded as 4-digit decimal number
	next if($len < MIN_LEN || $len > MAX_LEN);

	# Pointer to the next segment
	# starts at the RDR header
	my $ptr = $instdata->{'connections'}{$conn}{'buf_ptr'} + SIGNATURE_LEN;

	# Parse the rest of the RDR packet header
        my @RDR_header = unpack('x'.$ptr.' C2n2N2C',
				$instdata->{'connections'}{$conn}{'buffer'});

	# RDR header (minus TPID and length)
	# must have exactly 7 fields
	next unless scalar(@RDR_header) == HEADER_NUM_FIELDS;

	# Prepend TPID and length
	# to complete the header
	unshift @RDR_header, $len;
	unshift @RDR_header, $tpid;

	# Skip parsed header and set pointer to
	# the beginning of the RDR fields array
	$instdata->{'connections'}{$conn}{'buf_ptr'} = $ptr + HEADER_LEN;

	# Set local pointer to the end of RDR packet
	$ptr += $len;

	# Now that we have what we assume to be
	# a proper RDR header, we can move on to
	# the next phase - parsing actual data
	# TLVs.
	#
	# Packet can be received in chunks, so,
	# unless we have the rest of the packet
	# in the buffer already, we must change
	# this event's handler to the method
	# responsible for processing the TLVs.
	unless($instdata->{'connections'}{$conn}{'buf_len'} >= $ptr) {
	    # Set next phase's handler method
	    # as loop event handler.
	    #
	    # It will be delayed until the rest
	    # of the packet is received, or timeout
	    # has occured.
	    $instance->api->modify_loop_event($event,
					      'handler' => \&tlv_parser,
					      'args' => [ $instance, $instdata, $conn, \@RDR_header, $ptr ],
					      'delay' => 1);
	    # Done for now
	    return;
	}

	# If we have entire packet in the buffer,
	# go straight to the next phase ...
	return tlv_parser($event, $instance, $instdata, $conn, \@RDR_header, $ptr);
    }

    # Explicitly return nothing
    return;
}

sub tlv_parser($$$$$$) {
    my ($event, $instance, $instdata, $conn, $header, $end) = @_;

    # Buffer must contain entire packet before
    # we can start processing it
    unless($instdata->{'connections'}{$conn}{'buf_len'} -
		$instdata->{'connections'}{$conn}{'buf_ptr'} >= $header->[RDR_LEN]) {
	# If connection closed and buffer contains
	# no more complete packets ....
	if($instdata->{'connections'}{$conn}{'closed'}) {
	    # ... remove buffer parser from event monitor
	    $instance->api->destroy_loop_event($event);
	    # ... remove connection from our list
	    delete $instdata->{'connections'}{$conn};
	    # ... and, we are done
	    return;
	}
	# ... delay ourselves for 1 sec
	$instance->api->delay_event($event, 1);
	# ... do some debug logging
	$instance->api->logging('LOG_DEBUG', "RDR collector %s: not enough data in the buffer: TLV parser sleeping for 1 second",
					     $instance->{'instance'});
	# ... done for now
	return;
    }

    # Reset event handler to the header parsing phase
    $instance->api->modify_loop_event($event,
				      'handler' => \&header_parser,
				      'args' => [ $instance, $instdata, $conn ]);

    # If filter specifies RDR tags as match criteria,
    # RDR packet must match one of the tags
    if(defined($instdata->{'filter'}{'rdr'}) &&
       !($instdata->{'filter'}{'rdr'}{$header->[RDR_TAG]})) {
	# Skip entire filtered packet
	$instdata->{'connections'}{$conn}{'buf_ptr'} = $end;
	# Bail out
	return;
    }

    my @RDR_fields = ();

    # Parse RDR packet fields
    for(my $fc = 0, my $nf = int($header->[RDR_NUM_FIELDS]);
	($instdata->{'connections'}{$conn}{'buf_ptr'} < $end) && ($fc < $nf);
	$fc++) {
	# Parse field's TLV header
	my ($tlv_type, $tlv_len) = unpack('x'.$instdata->{'connections'}{$conn}{'buf_ptr'}.' CN',
					  $instdata->{'connections'}{$conn}{'buffer'});
	# TLV header format must have valid fields
	unless(defined($tlv_type) && $tlv_type =~ /^\d+$/ &&
	       defined($tlv_len) && $tlv_len =~ /^\d+$/) {
	    # Log invalid TLV header
	    $instance->api->logging('LOG_DEBUG', "RDR collector %s: received RDR packet with invalid TLV header from exporter %s",
						  $instance->{'instance'},
						  $conn->peerhost);
	    # Skip entire invalid packet
	    $instdata->{'connections'}{$conn}{'buf_ptr'} = $end;
	    # Bail out
	    return;
	}
	# Move buffer pointer to field's value
	$instdata->{'connections'}{$conn}{'buf_ptr'} += 5;
	# TLV length cannot exceed total packet length
	if($tlv_len > $end - $instdata->{'connections'}{$conn}{'buf_ptr'}) {
	    # Log invalid packet
	    $instance->api->logging('LOG_DEBUG', "RDR collector %s: received RDR packet with invalid TLV length from exporter %s",
						  $instance->{'instance'},
						  $conn->peerhost);
	    # Skip entire invalid packet
	    $instdata->{'connections'}{$conn}{'buf_ptr'} = $end;
	    # Bail out
	    return;
	}
	# Get unpack() format to be used for
	# parsing current RDR field's value
	my $format = $RDR_FIELD_FORMAT{$tlv_type};
	unless($format) {
	    # Log invalid TLV type
	    $instance->api->logging('LOG_DEBUG', "RDR collector %s: received RDR packet with unknown TLV type from exporter %s",
						  $instance->{'instance'},
						  $conn->peerhost);
	    # Skip entire invalid packet
	    $instdata->{'connections'}{$conn}{'buf_ptr'} = $end;
	    # Bail out
	    return;
	}
	# Finally, parse RDR field's value
	my $value = unpack('x'.$instdata->{'connections'}{$conn}{'buf_ptr'}.' '.
			   $format.(($tlv_type == RDR_TYPE_STRING) ? $tlv_len:''),
			   $instdata->{'connections'}{$conn}{'buffer'});
	# Unpack must return something,
	# otherwise value is invalid
	unless(defined($value)) {
	    # Log invalid value format
	    $instance->api->logging('LOG_DEBUG', "RDR collector %s: received RDR packet with invalid TLV value from exporter %s",
						  $instance->{'instance'},
						  $conn->peerhost);
	    # Skip entire invalid packet
	    $instdata->{'connections'}{$conn}{'buf_ptr'} = $end;
	    # Bail out
	    return;
	}
	# Next TLV
	$instdata->{'connections'}{$conn}{'buf_ptr'} += $tlv_len;
	# Store field's value
	push @RDR_fields, $value;
    }

    my @record = ();

    # Go through output record format definition
    foreach my $record_field (@{$instdata->{'record_format'}}) {
	# UNIX timestamp ?
	if($record_field eq 'timestamp') {
	    push @record, time();
	# RDR exporter ?
	} elsif($record_field eq 'exporter') {
	    push @record, $conn->peerhost;
	# RDR type ?
	} elsif($record_field eq 'rdr') {
	    # RDR type name defined ?
	    if(defined($instdata->{'tag'}{$header->[RDR_TAG]})) {
		push @record, $instdata->{'tag'}{$header->[RDR_TAG]};
	    }
	# RDR header field ?
	} elsif(defined($RDR_HEADER_FIELD{$record_field}) &&
		defined($header->[$RDR_HEADER_FIELD{$record_field}]) &&
		$header->[$RDR_HEADER_FIELD{$record_field}] ne '') {
	    push @record, $header->[$RDR_HEADER_FIELD{$record_field}];
	# RDR data field
	} elsif($record_field =~ /^\d+$/) {
	    push @record, $RDR_fields[$record_field];
	}
    }

    # Do some debug logging
    $instance->api->logging('LOG_DEBUG', "RDR collector %s: formatted output record %s",
					 $instance->{'instance'},
					 join(',', @record));

    # Put formatted record into output queue
    push @{$instdata->{'output_queue'}}, \@record;

    # Explicitly return nothing
    return;
}

1;

