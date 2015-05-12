#
# module::snmp.pm
#
# Copyright (c) 2013 Marko Dinic <marko@yu.net>. All rights reserved.
# This program is free software; you can redistribute it and/or
# modify it under the same terms as Perl itself.
#

package module::snmp;

##############################################################################################

use strict;
use warnings;

##############################################################################################

use SNMP;
use Config::ContextSensitive qw(:macros);

##############################################################################################

use api::module;

##############################################################################################

our @ISA = qw(api::module);

##############################################################################################

my $CONF_TEMPLATE = SECTION(
    DIRECTIVE('snmp_host', SECTION_NAME, SECTION(
	DIRECTIVE('snmp_community', ARG(CF_STRING, STORE(TO 'MODULE', KEY { '$SECTION[-2]' => { 'host' => { '$SECTION' => { 'community' => '$VALUE' } } } }))),
	DIRECTIVE('snmp_oid', REQUIRE(
	    DIRECTIVE('/^(gauge|counter|string)$/',
		ARG(CF_STRING, STORE(TO 'MODULE', KEY { '$SECTION[-2]' => { 'host' => { '$SECTION' => { 'oid' => { '$ARG[2]' => { 'type' => '$DIRECTIVE' } } } } } })),
	        ARG(CF_STRING, STORE(TO 'MODULE', KEY { '$SECTION[-2]' => { 'host' => { '$SECTION' => { 'oid' => { '$ARG[2]' => { 'asn' => '$VALUE' } } } } } })),
		ALLOW(
		    DIRECTIVE('index', ARG(CF_STRING, STORE(TO 'MODULE', KEY { '$SECTION[-2]' => { 'host' => { '$SECTION' => { 'oid' => { '$ARG[2]' => { 'index' => '$VALUE' } } } } } })))
		),
		ALLOW(
		    DIRECTIVE('every', ARG(CF_REAL|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION[-2]' => { 'host' => { '$SECTION' => { 'oid' => { '$ARG[2]' => { 'interval' => '$VALUE' } } } } } })))
		),
	    )
	)),
	DIRECTIVE('snmp_record_field', SKIP, ARG(CF_LINE, STORE(TO 'MODULE', KEY { '$SECTION[-2]' => { 'host' => { '$SECTION' => { 'field' => { '$ARG[1]' => { 'expr' => '$VALUE' } } } } } }))),
	DIRECTIVE('generate_snmp_record', SKIP, ALLOW(
	    DIRECTIVE('every', ARG(CF_REAL|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION[-2]' => { 'host' => { '$SECTION' => { 'record' => { '$ARG[1]' => { 'interval' => '$VALUE' } } } } } })))
	), ALLOW(
	    DIRECTIVE('index', ARG(CF_STRING, STORE(TO 'MODULE', KEY { '$SECTION[-2]' => { 'host' => { '$SECTION' => { 'record' => { '$ARG[1]' => { 'index' => '$VALUE' } } } } } })))
	), REQUIRE(
	    DIRECTIVE('format', ARG(CF_ARRAY, STORE(TO 'MODULE', KEY { '$SECTION[-2]' => { 'host' => { '$SECTION' => { 'record' => { '$ARG[1]' => { 'format' => '$VALUE' } } } } } })))
	)),
	DIRECTIVE('default_poll_interval', ARG(CF_REAL|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION[-2]' => { 'host' => { '$SECTION' => { 'default_poll_interval' => '$VALUE' } } } }), DEFAULT '10')),
	DIRECTIVE('snmp_query_timeout', ARG(CF_REAL|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION[-2]' => { 'host' => { '$SECTION' => { 'snmp_query_timeout' => '$VALUE' } } } }), DEFAULT '3')),
	DIRECTIVE('snmp_query_attempts', ARG(CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION[-2]' => { 'host' => { '$SECTION' => { 'snmp_query_attempts' => '$VALUE' } } } }), DEFAULT '3')),
	DIRECTIVE('snmp_table', SECTION_NAME, ALLOW(
	    DIRECTIVE('size', ARG(CF_INTEGER, STORE(TO 'MODULE', KEY { '$SECTION[-2]' => { 'host' => { '$SECTION' => { 'table' => { '$NESTED_SECTION' => { 'size' => '$VALUE' } } } } } }))),
	    DIRECTIVE('size_from', ARG(CF_STRING, STORE(TO 'MODULE', KEY { '$SECTION[-2]' => { 'host' => { '$SECTION' => { 'table' => { '$NESTED_SECTION' => { 'size_from' => '$VALUE' } } } } } })))
	), SECTION(
	    DIRECTIVE('table_refresh', ARG(CF_REAL|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION[-3]' => { 'host' => { '$SECTION[-2]' => { 'table' => { '$SECTION' => { 'refresh_interval' => '$VALUE' } } } } } }), DEFAULT '86400')),
	    DIRECTIVE('table_index', SKIP, REQUIRE(
		DIRECTIVE('matching', ARG(CF_STRING, STORE(TO 'MODULE', KEY { '$SECTION[-3]' => { 'host' => { '$SECTION[-2]' => { 'table' => { '$SECTION' => { 'index' => { '$ARG[1]' => '$VALUE' } } } } } } })))
	    ))
	))
    ))
);

##############################################################################################

our $WRAP32 = 0xffffffff + 1;

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

    # Initialize SNMP MIB
    SNMP::initMib();

    # Set global run flag
    $instdata->{'run'} = 1;

    # Initialize record output queue
    $instdata->{'output_queue'} = [];

    # Initialize instance data
    return $self->reinitialize($instdata);

}

sub reinitialize($$) {
    my ($self, $instdata) = @_;

    # (Re)init all configured hosts
    foreach my $host (keys %{$self->{'host'}}) {

	# Do some sanity checks ...
	foreach my $table (keys %{$self->{'host'}{$host}{'table'}}) {
	    # Go through table's indexes
	    foreach my $index_name (keys %{$self->{'host'}{$host}{'table'}{$table}{'index'}}) {
		# Look for index with the same name in other tables
		foreach my $other_table (keys %{$self->{'host'}{$host}{'table'}}) {
		    # Skip ourselves
		    next if $other_table eq $table;
		    # Configured index names must be unique
		    if(defined($self->{'host'}{$host}{'table'}{$other_table}{'index'}{$index_name})) {
			$self->api->logging('LOG_ERR', "SNMP collector %s: host %s already has index named %s in table %s. Index names must be unique.",
						       $self->{'instance'},
						       $host,
						       $index_name,
						       $table);
			return undef;
		    }
		}
	    }
	}

	# Initialize per-host data for new hosts
	unless(defined($instdata->{'host'}{$host})) {
	    # Init hash that will hold SNMP table indexes
	    $instdata->{'host'}{$host}{'indexes'} = {};
	}

	# Init per-host SNMP session
	$instdata->{'host'}{$host}{'snmp'} = SNMP::Session->new(DestHost => $host,
								Version => 2,
								Community => $self->{'host'}{$host}{'community'},
								Timeout => $self->{'host'}{$host}{'snmp_query_timeout'} * 1000000,
								Retries => $self->{'host'}{$host}{'snmp_query_attempts'},
								RetryNoSuch => 1);
	# SNMP session must be defined
	unless(defined($instdata->{'host'}{$host}{'snmp'})) {
	    $self->api->logging('LOG_ERR', "SNMP collector %s: failed to initialize SNMP session to host %s",
					   $self->{'instance'},
					   $host);
	    next;
	}

	# We will use random delay interval in attempt to
	# prevent SNMP query/response bursts while walking
	# the tables by randomly distributing walks over time
	my $table_delay = rand($self->{'host'}{$host}{'default_poll_interval'});

	# Go through all configured tables
	foreach my $table_oid (keys %{$self->{'host'}{$host}{'table'}}) {
	    # Create/modify periodic event for re-reading indexes for each table
	    $instdata->{'host'}{$host}{'table'}{$table_oid}{'indexer'} =
			defined($instdata->{'host'}{$host}{'table'}{$table_oid}{'indexer'}) ?
				$self->api->modify_timer_event($instdata->{'host'}{$host}{'table'}{$table_oid}{'indexer'},
							       'interval' => $self->{'host'}{$host}{'table'}{$table_oid}{'refresh_interval'},
							       'delay' => $table_delay,
							       'args' => [ $self, $instdata, $host, $table_oid ]):
				$self->api->create_timer_event('interval' => $self->{'host'}{$host}{'table'}{$table_oid}{'refresh_interval'},
							       'delay' => $table_delay,
							       'handler' => \&refresh_indexes,
							       'args' => [ $self, $instdata, $host, $table_oid ]);
	}

	# Create timer event for every OID we want to poll
	foreach my $oid_name (keys %{$self->{'host'}{$host}{'oid'}}) {
	    # This OID's poll interval:
	    #  - per OID, if defined
	    #  - per host otherwise
	    my $poll_interval = defined($self->{'host'}{$host}{'oid'}{$oid_name}{'interval'}) ?
						$self->{'host'}{$host}{'oid'}{$oid_name}{'interval'}:
						$self->{'host'}{$host}{'default_poll_interval'};
	    # We will use random delay interval in attempt to
	    # prevent SNMP query/response bursts by randomly
	    # distributing polls over time
	    my $poll_delay = $table_delay + rand($poll_interval);
	    # Create/modify periodic event for data retrieval
	    $instdata->{'host'}{$host}{'oid'}{$oid_name}{'poller'} =
		defined($instdata->{'host'}{$host}{'oid'}{$oid_name}{'poller'}) ?
			$self->api->modify_timer_event($instdata->{'host'}{$host}{'oid'}{$oid_name}{'poller'},
						       'interval' => $poll_interval,
						       'delay' => $poll_delay,
						       'args' => [ $self, $instdata, $host, $oid_name ]):
			$self->api->create_timer_event('interval' => $poll_interval,
						       'delay' => $poll_delay,
						       'handler' => \&poll_oid,
						       'args' => [ $self, $instdata, $host, $oid_name ]);
	}

	# Create timer event for every record format we want to generate
	foreach my $record_name (keys %{$self->{'host'}{$host}{'record'}}) {
	    # This record's generate/output interval
	    my $generate_interval = defined($self->{'host'}{$host}{'record'}{$record_name}{'interval'}) ?
						$self->{'host'}{$host}{'record'}{$record_name}{'interval'}:300;
	    # Create/modify periodic event for data output
	    $instdata->{'host'}{$host}{'record'}{$record_name}{'generator'} =
		defined($instdata->{'host'}{$host}{'record'}{$record_name}{'generator'}) ?
			$self->api->modify_timer_event($instdata->{'host'}{$host}{'record'}{$record_name}{'generator'},
						       'interval' => $generate_interval,
						       'delay' => $generate_interval,
						       'args' => [ $self, $instdata, $host, $record_name ]):
			$self->api->create_timer_event('interval' => $generate_interval,
						       'delay' => $generate_interval,
						       'handler' => \&generate_record,
						       'args' => [ $self, $instdata, $host, $record_name ]);
	}

    }

    # Hunt down removed configuration
    # elements' data and clean it up
    foreach my $host (keys %{$instdata->{'host'}}) {
	# If host is still in configuration ...
	if(defined($self->{'host'}{$host})) {
	    # ... check if it's configured tables have changed ...
	    foreach my $table_oid (keys %{$instdata->{'host'}{$host}{'table'}}) {
		# ... skipping still present ones
		next if defined($self->{'host'}{$host}{'table'}{$table_oid});
		# ... and cleaning up removed ones ...
		$self->api->destroy_timer_event($instdata->{'host'}{$host}{'table'}{$table_oid}{'indexer'});
		delete $instdata->{'host'}{$host}{'table'}{$table_oid};
	    }
	    # ... check if it's configured OIDs have changed ...
	    foreach my $oid (keys %{$instdata->{'host'}{$host}{'oid'}}) {
		# ... skipping still present ones
		next if defined($self->{'host'}{$host}{'oid'}{$oid});
		# ... and cleaning up removed ones
		$self->api->destroy_timer_event($instdata->{'host'}{$host}{'oid'}{$oid}{'poller'});
		delete $instdata->{'host'}{$host}{'oid'}{$oid};
	    }
	    # ... check if it's configured records have changed ...
	    foreach my $record_name (keys %{$instdata->{'host'}{$host}{'record'}}) {
		# ... skipping still present ones
		next if defined($self->{'host'}{$host}{'record'}{$record_name});
		# ... and cleaning up removed ones
		$self->api->destroy_timer_event($instdata->{'host'}{$host}{'record'}{$record_name}{'generator'});
		delete $instdata->{'host'}{$host}{'record'}{$record_name};
	    }
	# Otherwise, if host has been removed ...
	} else {
	    # ... stop all indexers associated with this host ...
	    foreach my $table_oid (keys %{$instdata->{'host'}{$host}{'table'}}) {
		# ... destroy timer event that implements table walker ...
		$self->api->destroy_timer_event($instdata->{'host'}{$host}{'table'}{$table_oid}{'indexer'});
	    }
	    # ... stop all pollers associated with this host ...
	    foreach my $oid_name (keys %{$instdata->{'host'}{$host}{'oid'}}) {
		# ... destroy timer event that implements SNMP poller ...
		$self->api->destroy_timer_event($instdata->{'host'}{$host}{'oid'}{$oid_name}{'poller'});
	    }
	    # ... stop all record generators associated with this host ...
	    foreach my $record_name (keys %{$instdata->{'host'}{$host}{'record'}}) {
		# ... destroy timer event that implements record generator ...
		$self->api->destroy_timer_event($instdata->{'host'}{$host}{'record'}{$record_name}{'generator'});
	    }
	    # ... and finally delete host's data
	    delete $instdata->{'host'}{$host};
	}
    }

    # Create/modify main SNMP event processor
    $instdata->{'dispatcher'} = defined($instdata->{'dispatcher'}) ?
		    $self->api->modify_loop_event($instdata->{'dispatcher'},
						  'args' => [ $self, $instdata ]):
		    $self->api->create_loop_event('handler' => \&dispatch_requests,
						  'args' => [ $self, $instdata ]);

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

sub abort($$) {
    my ($self, $instdata) = @_;

    # Bring run flag down
    $instdata->{'run'} = 0;

    # Close all SNMP sessions
    $self->cleanup($instdata);
}

sub cleanup($$) {
    my ($self, $instdata) = @_;

    # Destroy all SNMP sessions all configured hosts
    foreach my $host (keys %{$instdata->{'host'}}) {
	# If session exists ...
	if(defined($instdata->{'host'}{$host}{'snmp'})) {
	    # ... close it and remove any trace of it
	    undef $instdata->{'host'}{$host}{'snmp'};
	    delete $instdata->{'host'}{$host}{'snmp'};
	}
    }
}

##############################################################################################

sub dispatch_requests($$$) {
    my ($event, $instance, $instdata) = @_;

    # Run main SNMP collect loop
    # in 10ms time slices
    SNMP::MainLoop(0.01, sub { SNMP::finish(); });

    # Explicitly return nothing
    return;
}

##############################################################################################

sub queue_runner($$$) {
    my ($channel, $instance, $instdata) = @_;

    # If we got no records pending ...
    unless(@{$instdata->{'output_queue'}}) {
	# ... delay ourselves for 1 sec
	$instance->api->delay_event($instance->api->get_event($channel, 'w'), 0.01);
	# ... do some debug logging
	$instance->api->logging('LOG_DEBUG', "SNMP collector %s: output queue is empty: queue runner sleeping for 10 milisecond",
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

sub refresh_indexes($$$$$) {
    my ($timer, $instance, $instdata, $host, $table_oid) = @_;

    # Get table size from an OID ?
    if(defined($instance->{'host'}{$host}{'table'}{$table_oid}{'size_from'}) &&
       $instance->{'host'}{$host}{'table'}{$table_oid}{'size_from'} ne '') {

	# This is the OID that holds the current size of the table
	my $size_oid = $instance->{'host'}{$host}{'table'}{$table_oid}{'size_from'};

	# Queue table size request
	$instdata->{'host'}{$host}{'snmp'}->get(SNMP::VarList->new([$size_oid]), sub {
		# Get SNMP results
		my $varlist = shift;
		unless(defined($varlist)) {
		    $instance->api->logging('LOG_ERR', "SNMP collector %s: failed to get size of table %s from host %s",
						       $instance->{'instance'},
						       $table_oid,
						       $host);
		    return;
		}
		# Fetch table using maximum size read from size oid
		return $instance->request_table($instdata,
						$host,
						$table_oid,
						$varlist->[0]->val);
	});

	return;
    }

    # Fetch table using statically defined
    # maximum size or no maximum size at all
    return $instance->request_table($instdata,
				    $host,
				    $table_oid,
				    $instance->{'host'}{$host}{'table'}{$table_oid}{'size'});

}

sub request_table($$$$;$) {
    my ($instance, $instdata, $host, $table_oid, $table_size) = @_;
    my %size;

    if(defined($table_size) && $table_size > 0) {
	# Set the number of entries to walk to retrieved table size
	%size = (repeatcount => $table_size);
    }

    # Request table contents (walk the table)
    $instdata->{'host'}{$host}{'snmp'}->gettable($table_oid,
						 %size,
						 noindexes => 1,
						 columns => [ $table_oid ],
						 callback => sub {
	# Get the table
	my $table = shift;
	unless(defined($table)) {
	    $instance->api->logging('LOG_ERR', "SNMP collector %s: failed to get the table %s from host %s",
					       $instance->{'instance'},
					       $table_oid,
					       $host);
	    return;
	}
	# Process table
	return $instance->process_table($instdata,
					$host,
					$table_oid,
					$table);
    });

    return;
}

sub process_table($$$$$) {
    my ($instance, $instdata, $host, $table_oid, $table) = @_;

    # Loop through all configured indexes for current table
    foreach my $index_name (keys %{$instance->{'host'}{$host}{'table'}{$table_oid}{'index'}}) {
	# This is where we collect table indexes
	my @indexes = ();
	# Regexp matching our index
	my $regexp = $instance->{'host'}{$host}{'table'}{$table_oid}{'index'}{$index_name};
	# Search for index whose value
	# matches the configured regexp
	foreach my $index (keys %{$table}) {
	    my ($value) = (values %{$table->{$index}});
	    # Match OID data against regexp
	    if($value =~ /$regexp/i) {
		# Keep extracted index
		push @indexes, $index;
		# Do some debug logging
		$instance->api->logging('LOG_DEBUG', "SNMP collector %s: index %s matching \"%s\" in table %s is %u",
						     $instance->{'instance'},
						     $index_name,
						     $regexp,
						     $table_oid,
						     $index);
	    }
	}
	# If we have at least one match, we're ok
	if(@indexes) {
	    # Store all matching indexes
	    # under the same index name
	    $instdata->{'host'}{$host}{'indexes'}{$index_name} = \@indexes;
	# Report missing index
	} else {
	    $instance->api->logging('LOG_WARNING', "SNMP collector %s: host %s doesn't have any index matching \"%s\" in table %s",
						   $instance->{'instance'},
						   $host,
						   $regexp,
						   $table_oid);
	}
    }

    # Prepare the list of OIDs to request from the device
    foreach my $oid_name (keys %{$instance->{'host'}{$host}{'oid'}}) {
	# Get specified (base) OID
	my $oid = $instance->{'host'}{$host}{'oid'}{$oid_name}{'asn'};
	# This is where we build the list of OIDs
	# we will request from hosts on each run
	my $varlist = [];
	# Is it a table base OID ?
	my $index_name = $instance->{'host'}{$host}{'oid'}{$oid_name}{'index'};
	if(defined($index_name)) {
	    # Index the table's base OID, 
	    # producing a list of instances
	    # that represent table entries
	    foreach my $index (@{$instdata->{'host'}{$host}{'indexes'}{$index_name}}) {
		# Append this OID instance to the list of OIDs
		# that will be requested from the host
		push @{$varlist}, SNMP::Varbind->new([$oid, $index]);
	    }
	} else {
	    # Append this OID to the list of OIDs
	    # that we will request from the device
	    push @{$varlist}, SNMP::Varbind->new([$oid]);
	}
	# Set the new var bind list for this OID
	$instdata->{'host'}{$host}{'oid'}{$oid_name}{'varlist'} = $varlist;
    }

    return;
}

##############################################################################################

sub poll_oid($$$$$) {
    my ($timer, $instance, $instdata, $host, $oid_name) = @_;

    # Can't do anything with an empty var bind list
    unless(defined($instdata->{'host'}{$host}{'oid'}{$oid_name}{'varlist'}) &&
	   scalar(@{$instdata->{'host'}{$host}{'oid'}{$oid_name}{'varlist'}}) > 0) {
	# Do some debug logging
	$instance->api->logging('LOG_DEBUG', "SNMP collector %s: host %s still waiting on varbind list for OID %s",
					     $instance->{'instance'},
					     $host,
					     $oid_name);
	return;
    }

    # Split full list of OIDs we want into small chunks
    # that we will queue as multiple requests.

    # This is our working copy of the var bind list
    my @varlist = @{$instdata->{'host'}{$host}{'oid'}{$oid_name}{'varlist'}};
    # Queue requests chunk by chunk
    while(@varlist) {
	# Next chunk of the list to be queued
	my @request_vars = splice(@varlist, 0, 10);
	# Request SNMP data from the device
	$instdata->{'host'}{$host}{'snmp'}->get(SNMP::VarList->new(@request_vars), sub {
		# Get SNMP response data
		my $data = shift;
		unless(defined($data)) {
		    $instance->api->logging('LOG_ERR', "SNMP collector %s: failed to collect data from host %s",
						       $instance->{'instance'},
						       $host);
		    return;
		}
		# Merge received data into single hash
		foreach my $varbind (@{$data}) {
		    # Make sure OID is in numeric form
		    my $oid = ($varbind->tag =~ /[^\.\d]/) ?
				SNMP::translateObj($varbind->tag):$varbind->tag;
		    # If OID is a table, instance id
		    # will be given separately ...
		    my $inst = $varbind->iid;
		    if(defined($inst) && $inst ne '') {
			# Put base oid and instance id together
			$oid .= '.'.$varbind->iid;
		    }
		    # OIDs current value
		    my $value = $varbind->val;
		    # Destroy value if we got NOSUCHINSTANCE,
		    # because it basically means we got undef
		    undef $value if $value =~ /^nosuchinstance$/i;
		    # OID is configured as counter ?
		    if(defined($value) && $instance->{'host'}{$host}{'oid'}{$oid_name}{'type'} eq 'counter') {
			# If wrap counter isn't defined yet ...
			unless(defined($instdata->{'host'}{$host}{'oid'}{$oid_name}{'wrapcnt'}{$oid})) {
			    # ... define it and start from zero
			    $instdata->{'host'}{$host}{'oid'}{$oid_name}{'wrapcnt'}{$oid} = 0;
			}
			# Since counters are ever-increasing,
			# if new value is less than previous one,
			# we got a counter wrap
			if(defined($instdata->{'host'}{$host}{'oid'}{$oid_name}{'data'}{$oid}) &&
			   ($value < $instdata->{'host'}{$host}{'oid'}{$oid_name}{'data'}{$oid})) {
			    # Count how many times the counter
			    # wrapped around 32-bit max value
			    $instdata->{'host'}{$host}{'oid'}{$oid_name}{'wrapcnt'}{$oid}++;
			}
		    }
		    # Store retrieved data for later use in record field expressions
		    $instdata->{'host'}{$host}{'oid'}{$oid_name}{'data'}{$oid} = $value;
		}
		# Do some debug logging
		$instance->api->logging('LOG_DEBUG', "SNMP collector %s: host %s returned OIDs (%s)",
						     $instance->{'instance'},
						     $host,
						     join(' ', (keys %{$instdata->{'host'}{$host}{'oid'}{$oid_name}{'data'}})));
		return;
	});
    }

    # Explicitly return nothing
    return;
}

##############################################################################################

sub generate_record($$$$) {
    my ($timer, $instance, $instdata, $host, $record_name) = @_;

    # Get the name of table index (if defined) that will be used
    # to index any table OID referenced by fields of current record
    # producing a bunch of table OID instances
    my $index_name = $instance->{'host'}{$host}{'record'}{$record_name}{'index'};

    # This is either a list of indexes
    # or (almost) an empty list
    my $indexes = defined($index_name) ?
			$instdata->{'host'}{$host}{'indexes'}{$index_name}:[undef];

    # Format as many output records as there are table indexes
    foreach my $index (@{$indexes}) {
	# Begin a new record
	my @record  = ();
	# Sort retrieved values in the same order
	# fields are defined in the record format
	foreach my $field_name (@{$instance->{'host'}{$host}{'record'}{$record_name}{'format'}}) {
	    # Format record field
	    my $value =  $instance->eval_expression($instdata,
						    $host,
						    $field_name,
						    $index);
	    # Skip this record/index entirely
	    # if we failed to format one of
	    # the data record fields ...
	    unless(defined($value)) {
		# ... clear the record
		@record = ();
		# ... and bail out
		last;
	    }
	    # Store field value in the record
	    push @record, $value;
	}

	# Skip empty record
	unless(@record) {
	    $instance->api->logging('LOG_DEBUG', "SNMP collector %s: skipping data at index %d collected from host %s",
						 $instance->{'instance'},
						 $index,
						 $host);
	    next;
	}

	# Do some debug logging
	$instance->api->logging('LOG_DEBUG', "SNMP collector %s: completed data record from host %s [%s]: %s",
					     $instance->{'instance'},
					     $host,
					     join(',', @{$instance->{'host'}{$host}{'record'}{$record_name}{'format'}}),
					     join(',', @record));

	# Queue produced record for output
	push @{$instdata->{'output_queue'}}, \@record;
    }

    return;
}

##############################################################################################

sub eval_expression($$$$;$) {
    my ($self, $instdata, $host, $field_name, $index) = @_;
    my $result;

    # Expression to evaluate
    my $expr = $self->{'host'}{$host}{'field'}{$field_name}{'expr'};
    my $orig_expr = $expr;

    # Replace $oid_names with oid values
    while($orig_expr =~ /(([\$\@%])([^\$\s%]+))/g) {
	my $variable = $1;
	my $var_name = $3;
	my $var_type = $2;
	my $var_sub;
	my $value;
	# Internal variable ?
	if($var_type eq '%') {
	    # Current time (UNIX timestamp) ?
	    if($var_name eq 'time') {
		# Replacement value will be current timestamp
		$value = int(time());
	    } elsif($var_name eq 'host') {
		# Replacement value will be host
		$value = '"'.$host.'"';
	    } elsif($var_name eq 'index') {
		# Replacement value will be index
		$value = $index;
		unless(defined($value) && $value =~ /^\d+$/) {
		    $self->api->logging('LOG_ERR', "SNMP collector %s: host %s: expression (%s) is referencing invalid index",
						   $self->{'instance'},
						   $host,
						   $orig_expr);
		    return undef;
		}
	    } else {
		$self->api->logging('LOG_ERR', "SNMP collector %s: host %s: expression (%s) is referencing unknown internal variable %s",
					       $self->{'instance'},
					       $host,
					       $orig_expr,
					       $variable);
		return undef;
	    }
	# Otherwise it is counter/gauge/string variable ...
	} else {
	    # OID can reference a single value
	    # or a SNMP table entry value
	    my $oid = (defined($index) && 
		       defined($self->{'host'}{$host}{'oid'}{$var_name}{'index'})) ?
				    $self->{'host'}{$host}{'oid'}{$var_name}{'asn'}.'.'.$index:
				    $self->{'host'}{$host}{'oid'}{$var_name}{'asn'};
	    # OID has to be defined
	    unless(defined($oid) && $oid ne '') {
		$self->api->logging('LOG_ERR', "SNMP collector %s: host %s: expression (%s) is referencing unknown OID variable %s",
					       $self->{'instance'},
					       $host,
					       $orig_expr,
					       $variable);
		return undef;
	    }
	    # Get OID's value
	    $value = $instdata->{'host'}{$host}{'oid'}{$var_name}{'data'}{$oid};
	    # If we didn't receive this OID's data
	    # or device reported OID as undefined ...
	    unless(defined($value)) {
		# ... log missing OID
		$self->api->logging('LOG_DEBUG', "SNMP collector %s: host %s: expression (%s) is referencing missing OID %s via OID variable %s",
						 $self->{'instance'},
						 $host,
						 $orig_expr,
						 $oid,
						 $variable);
		# ... and bail out
		return undef;
	    }
	    # Variable is counter ?
	    if($self->{'host'}{$host}{'oid'}{$var_name}{'type'} eq 'counter') {
		# Result is OID's value from received data
		unless($value =~ /^\d+(\.\d+)?$/) {
		    $self->api->logging('LOG_ERR', "SNMP collector %s: host %s: expression (%s) is referencing invalid counter value (%s) via OID variable %s (OID %s)",
						   $self->{'instance'},
						   $host,
						   $orig_expr,
						   $value,
						   $variable,
						   $oid);
		    return undef;
		}
		# Compensate for 32-bit counter wrap
		$value += defined($instdata->{'host'}{$host}{'oid'}{$var_name}{'wrapcnt'}{$oid}) ?
				    ($WRAP32 * $instdata->{'host'}{$host}{'oid'}{$var_name}{'wrapcnt'}{$oid}):0;
	    # Variable is gauge ?
	    } elsif($self->{'host'}{$host}{'oid'}{$var_name}{'type'} eq 'gauge') {
		# Result is OID's value from received data
		unless($value =~ /^\d+(\.\d+)?$/) {
		    $self->api->logging('LOG_ERR', "SNMP collector %s: host %s: expression (%) is referencing invalid gauge value (%s) via OID variable %s (OID %s)",
						   $self->{'instance'},
						   $host,
						   $orig_expr,
						   $value,
						   $variable,
						   $oid);
		    return undef;
		}
	    # Variable is string ?
	    } elsif($self->{'host'}{$host}{'oid'}{$var_name}{'type'} eq 'string') {
		# Result is a quoted string
		unless($value ne '') {
		    $self->api->logging('LOG_ERR', "SNMP collector %s: host %s: expression (%s) is referencing empty string value via OID variable %s (OID %s)",
						   $self->{'instance'},
						   $host,
						   $orig_expr,
						   $variable,
						   $oid);
		    return undef;
		}
		# Make string quoted
		$value = '"'.$value.'"';
	    }
	}
	# Escape variable prefix
	$variable = '\\'.$variable;
	# Replace variable name with actual retrieved value
	$expr =~ s/$variable/$value/g;
    }

    # Evaluate the expression
    eval '$result='.$expr;

    # Do some debug logging
    $self->api->logging('LOG_DEBUG', "SNMP collector %s: host %s: xlat (%s) to (%s) which evaluates to %s",
				     $self->{'instance'},
				     $host,
				     $orig_expr,
				     $expr,
				     $result);

    return $result;
}

1;
