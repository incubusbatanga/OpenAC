#
# module::csv.pm
#
# Copyright (c) 2012 Marko Dinic <marko@yu.net>. All rights reserved.
# This program is free software; you can redistribute it and/or
# modify it under the same terms as Perl itself.
#

package module::csv;

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
    DIRECTIVE('listen_on_port', ARG(CF_PORT, STORE(TO 'MODULE', KEY { '$SECTION' => { 'listen_port' => '$VALUE' } }), DEFAULT '7070')),
    DIRECTIVE('record_format', ARG(CF_ARRAY|CF_MAPPED, FROM 'DATA_TYPES', STORE(TO 'MODULE', KEY { '$SECTION' => { 'record_format' => '$VALUE' } }))),
    DIRECTIVE('num_fields', ARG(CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION' => { 'num_fields' => '$VALUE' } }))),
    DIRECTIVE('unique_fields', ARG(CF_ARRAY|CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION' => { 'unique_fields' => '$VALUE' } }))),
    DIRECTIVE('unique_timeframe', ARG(CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION' => { 'record_lifetime' => '$VALUE' } } ), DEFAULT '300'))
);

##############################################################################################

our %DATA_TYPES = (
    'string'		=> CF_STRING,
    'integer'		=> CF_INTEGER,
    'float'		=> CF_REAL,
    'timestamp'		=> CF_INTEGER|CF_POSITIVE|CF_NONZERO,
    'interval'		=> CF_INTEGER|CF_POSITIVE,
    'counter'		=> CF_INTEGER|CF_POSITIVE,
    'gauge'		=> CF_INTEGER|CF_POSITIVE,
    'path'		=> CF_PATH
);

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

    # This is where we keep our established connections
    $instdata->{'connections'} = {};

    # Cache that weeds out duplicate records by keeping
    # received record data for next N seconds to be
    # compared to other incoming records
    my $records_cache = api::util::lru->new(1);
    unless(defined($records_cache)) {
	$self->api->logging('LOG_ERR', "CSV collector %s: failed to create unique records cache",
				       $self->{'instance'});
	return undef;
    }

    # Store cache object
    $instdata->{'records_cache'} = $records_cache;

    # If record uniqueness if is required,
    # records cache will be maintained.
    if(defined($self->{'unique_fields'})) {
	# Create timer event that ticks once per second, removing
	# expired unique record entries from the records cache
	$self->api->create_timer_event('interval' => 1,
				       'handler' => \&record_reaper,
				       'args' => [ $instdata ]);
    }

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
	    $self->api->logging('LOG_ERR', "CSV collector %s: failed to create listener socket",
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

    return $instdata;
}

sub cleanup($$) {
    my ($self, $instdata) = @_;

    foreach my $conn (keys %{$instdata->{'connections'}}) {
	# Remove socket from event monitor
	$self->api->destroy_io_event($conn, 'r');
	# Close socket
	close($conn);
	# Remove it from our list
	delete $instdata->{'connections'}{$conn};
    }

    # Remove listener from event monitor
    $self->api->destroy_io_event($instdata->{'listener'}, 'r');
    # Close listener socket
    close($instdata->{'listener'});
    # Remove it from our list
    delete $instdata->{'listener'};

}


##############################################################################################

sub socket_listener($$$) {
    my ($listener, $instance, $instdata) = @_;

    # Accept incoming connection
    my $conn = $listener->accept();

    unless(defined($conn)) {
	$instance->api->logging('LOG_ERR', "CSV collector %s: failed to accept incoming connection",
					   $instance->{'instance'});
	return;
    }

    # Make socket non-blocking
    $instance->api->set_nonblocking($conn);
    # Save the connection socket
    $instdata->{'connections'}{$conn} = 1;

    # Create I/O event that reads from network
    $instance->api->create_io_event('file' => $conn,
				    'op' => 'r',
				    'handler' => \&network_receiver,
				    'args' => [ $instance, $instdata ]);

    $instance->api->logging('LOG_INFO', "CSV collector %s: exporter %s connected",
					$instance->{'instance'},
					$conn->peerhost);

    return;
}

sub network_receiver($$$) {
    my ($conn, $instance, $instdata) = @_;

    # Read as much records as possible
    my $csvrecord = <$conn>;

    # Connection closed ?
    unless(defined($csvrecord)) {
	$instance->api->logging('LOG_INFO', "CSV collector %s: exporter %s disconnected",
					    $instance->{'instance'},
					    $conn->peerhost);
	# Remove socket from event monitor
	$instance->api->destroy_io_event($conn, 'r');
	# Close socket
	close($conn);
	# Remove it from our list
	delete $instdata->{'connections'}{$conn};
	# Done with this connection
	return;
    }

    # Ignore empty lines
    unless($csvrecord ne '') {
	# Log invalid record
	$instance->api->logging('LOG_DEBUG', "CSV collector %s: received empty line from exporter %s",
					     $instance->{'instance'},
					     $conn->peerhost);
	return;
    }

    my @record = ();

    while($csvrecord =~ /(?:\'([^\'\n\r]*)\'|\"([^\"\n\r]*)\"|([^,\'\"\n\r]*))(?:,|[\n\r]+)/g) {
	if(defined($1)) {
	    push @record, $1;
	} elsif(defined($2)) {
	    push @record, $2;
	} elsif(defined($3)) {
	    push @record, $3;
	} else {
	    # Log invalid record
	    $instance->api->logging('LOG_DEBUG', "CSV collector %s: received invalid CSV line from exporter %s",
						 $instance->{'instance'},
						 $conn->peerhost);
	    return;
	}
    }

    # If number of fields per record or/and record format
    # are explicitly defined, records must always have
    # that exact number of fields
    if((defined($instance->{'num_fields'}) &&
        scalar(@record) != $instance->{'num_fields'}) ||
       (defined($instance->{'record_format'}) &&
        scalar(@record) != scalar(@{$instance->{'record_format'}}))) {
	# Log invalid record
	$instance->api->logging('LOG_DEBUG', "CSV collector %s: received data record with wrong number of fields (%u instead of %u) from exporter %s",
					     $instance->{'instance'},
					     scalar(@record),
					     $instance->{'num_fields'},
					     $conn->peerhost);
	return;
    }

    # Hopefully, this flag will remain TRUE
    my $match = 1;

    # Verify record format
    for(my $i = 0; $i < scalar(@{$instance->{'record_format'}}); $i++) {
	# Get field contents and type
	my $field = $record[$i];
	my $type = $instance->{'record_format'}[$i];
	# Is field supposed to be integer ?
	if($type & CF_INTEGER) {
	    $match = ($field =~ /^[\+\-]?\d+/);
	    last unless $match;
	    if($type & CF_POSITIVE) {
		$match = ($field >= 0);
		last unless $match;
		if($type & CF_NONZERO) {
		    $match = ($field > 0);
		    last unless $match;
		}
	    }
	# Is field supposed to be float ?
	} elsif($type & CF_REAL) {
	    $match = ($field =~ /^[\+\-]?\d+?(?:\.\d+)?/);
	    last unless $match;
	    if($type & CF_POSITIVE) {
		$match = ($field >= 0);
		last unless $match;
		if($type & CF_NONZERO) {
		    $match = ($field > 0);
		    last unless $match;
		}
	    }
	# Is field a filesystem path ?
	} elsif($type & CF_PATH) {
	    $match = ($field =~ /^\/?(?:[^\/]+\/)+(?:[^\/]+)?$/);
	    last unless $match;
	}
    }
    # On no match, consider record invalid,
    # log error and discard the record
    unless($match) {
	# Log invalid record
	$instance->api->logging('LOG_DEBUG', "CSV collector %s: received wrong data record from exporter %s (%s)",
					     $instance->{'instance'},
					     $conn->peerhost,
					     join(',', @record));
	return;
    }

    # When incoming records can be duplicates,
    # a list of fields that uniquely identify
    # a record should be defined to weed out
    # records with the same set of fields.
    if(defined($instance->{'unique_fields'})) {
	# Format unique key
	my $unique_key = '';
	foreach my $index (@{$instance->{'unique_fields'}}) {
	    $unique_key .= $record[$index-1];
	}
	# We must make sure record with same data
	# hasn't already been received ...
	if($instdata->{'records_cache'}->exists($unique_key)) {
	    # Log duplicate record
	    $instance->api->logging('LOG_DEBUG', "CSV collector %s: ignoring duplicate data record from exporter %s (%s)",
						 $instance->{'instance'},
						 $conn->peerhost,
						 join(',', @record));
	    return;
	}
	# For next N seconds ignore all records
	# with same data (except timestamp)
	$instdata->{'records_cache'}->insert($unique_key,
					     $instance->{'record_lifetime'});
    }

    # Deliver the record to the main process
    return (@record);
}

sub record_reaper($$) {
    my ($event, $instdata) = @_;

    # Remove unique records that have expired
    # (max a 100 at a time)
    for(my $i = 100; $i ; $i--) {
	return unless $instdata->{'records_cache'}->expired;
    }

    return;
}

1;
