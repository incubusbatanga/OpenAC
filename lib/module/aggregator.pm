#
# module::aggregator.pm
#
# Copyright (c) 2012 Marko Dinic <marko@yu.net>. All rights reserved.
# This program is free software; you can redistribute it and/or
# modify it under the same terms as Perl itself.
#

package module::aggregator;

##############################################################################################

use strict;
use warnings;

##############################################################################################

use Time::HiRes qw(time);
use Config::ContextSensitive qw(:macros);

##############################################################################################

use api::module;

##############################################################################################

our @ISA = qw(api::module);

##############################################################################################

my $CONF_TEMPLATE = SECTION(
    DIRECTIVE('bucket_group', SECTION_NAME, SECTION(
	DIRECTIVE('only_records', REQUIRE(
	    DIRECTIVE('with', REQUIRE(
		DIRECTIVE('field', SKIP(CF_INTEGER|CF_POSITIVE|CF_NONZERO), REQUIRE(
		    DIRECTIVE('matching', ARG(CF_STRING, STORE(TO 'MODULE', KEY { '$PARENT_SECTION' => { 'bucket_group' => { '$SECTION' => { 'filter' => { '$ARGR[-2]' => '$VALUE' } } } } })))
		))
	    ))
	)),
	DIRECTIVE('unique_session', REQUIRE(
	    DIRECTIVE('key', REQUIRE(
		DIRECTIVE('fields', ARG(CF_ARRAY|CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$PARENT_SECTION' => { 'bucket_group' => { '$SECTION' => { 'session_key_fields' => '$VALUE' } } } })))
	    )),
	    DIRECTIVE('idle', REQUIRE(
		DIRECTIVE('timeout', ARG(CF_STRING, STORE(TO 'MODULE', KEY { '$PARENT_SECTION' => { 'bucket_group' => { '$SECTION' => { 'session_idle_timeout' => '$VALUE' } } } })))
	    ))
	)),
	DIRECTIVE('bucket', REQUIRE(
	    DIRECTIVE('key', REQUIRE(
		DIRECTIVE('fields', ARG(CF_ARRAY|CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$PARENT_SECTION' => { 'bucket_group' => { '$SECTION' => { 'bucket_key_fields' => '$VALUE' } } } })))
	    )),
	    DIRECTIVE('idle', REQUIRE(
		DIRECTIVE('timeout', ARG(CF_STRING, STORE(TO 'MODULE', KEY { '$PARENT_SECTION' => { 'bucket_group' => { '$SECTION' => { 'bucket_idle_timeout' => '$VALUE' } } } }))),
	    )),
	    DIRECTIVE('field', SKIP, REQUIRE(
		DIRECTIVE('type', REQUIRE(
		    DIRECTIVE('/^(volume|rate)$/', REQUIRE(
			DIRECTIVE('input_field', ARG(CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$PARENT_SECTION' => { 'bucket_group' => { '$SECTION' => { 'bucket_field' => { '$ARG[2]' => { 'type' => '$ARG[4]', 'input_field' => { 'index' => '$VALUE' } } } } } } })), REQUIRE(
			    DIRECTIVE('type', REQUIRE(
				DIRECTIVE('/^(volume|rate)$/', OPER(STORE(TO 'MODULE', KEY { '$PARENT_SECTION' => { 'bucket_group' => { '$SECTION' => { 'bucket_field' => { '$ARG[2]' => { 'input_field' => { 'type' => '$DIRECTIVE' } } } } } } })))
			    ))
			))
		    ))
		))
	    )),
	    DIRECTIVE('output', REQUIRE(
		DIRECTIVE('drift', ARG(CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$PARENT_SECTION' => { 'bucket_group' => { '$SECTION' => { 'bucket_output' => { 'drift' => '$VALUE' } } } } }))),
		DIRECTIVE('every', ARG(CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$PARENT_SECTION' => { 'bucket_group' => { '$SECTION' => { 'bucket_output' => { 'interval' => '$VALUE' } } } } })), REQUIRE(
		    DIRECTIVE('format', ARG(CF_ARRAY, STORE(TO 'MODULE', KEY { '$PARENT_SECTION' => { 'bucket_group' => { '$SECTION' => { 'bucket_output' => { 'format' => '$VALUE' } } } } })))
		))
	    ))
	))
    ))
);

##############################################################################################

sub register() {
    # Return our configuration template
    return $CONF_TEMPLATE;
}

sub daemonize($) {
    my $self = shift;

    # Init instance private data
    my $instdata = { 'bucket_group' => {} };

    return $instdata;
}

sub initialize($$) {
    my ($self, $instdata) = @_;

    # Initialize bucket groups
    return $self->init_bucket_groups($instdata)
}

sub reinitialize($$) {
    my ($self, $instdata) = @_;

    # Remove bucket groups that have been
    # removed from configuration ...
    foreach my $group_name (keys %{$instdata->{'bucket_group'}}) {
	# ... skip for groups that still exist
	# in the new configuration. Otherwise ...
	next if defined($self->{'bucket_group'}{$group_name});
	# ... destroy removed group's worker
	$self->api->destroy_loop_event($instdata->{'bucket_group'}{$group_name}{'worker'});
	# ... destroy bucket group's session index
	undef $instdata->{'bucket_group'}{$group_name}{'sessions'};
	# ... destroy bucket group's records cache
	undef $instdata->{'bucket_group'}{$group_name}{'records_cache'};
	# ... destroy bucket group's buckets
	undef $instdata->{'bucket_group'}{$group_name}{'bucket'};
	# ... destroy removed bucket group
	undef $instdata->{'bucket_group'}{$group_name};
	delete $instdata->{'bucket_group'}{$group_name};
    }

    # Initialize bucket groups
    return $self->init_bucket_groups($instdata)
}

sub process($$;@) {
    my $self = shift;
    my $instdata = shift;

    # Get current timestamp
    my $current_time = time();

    # Offer received record to all configured bucket groups
    foreach my $group_name (keys %{$self->{'bucket_group'}}) {

	# Hopefully, this flag will remain up
	my $match = 1;
	# Run fields through filter expressions to select
	# which records go into this bucket group
	foreach my $field_index (keys %{$self->{'bucket_group'}{$group_name}{'filter'}}) {
	    # Regexp to match specified field against
	    my $match_expr = $self->{'bucket_group'}{$group_name}{'filter'}{$field_index};
	    # Every field must match
	    unless(defined($match_expr) &&
		   $match_expr ne '' &&
		   defined($_[$field_index-1]) &&
		   $_[$field_index-1] =~ /$match_expr/) {
		# Match failed
		$match = 0;
		last;
	    }
	}
	# This bucket group will ignore record
	# if filter was defined but match failed
	next unless $match;

	# Format bucket key
	my $bucket_key = (defined($self->{'bucket_group'}{$group_name}{'bucket_key_fields'}) &&
			  scalar(@{$self->{'bucket_group'}{$group_name}{'bucket_key_fields'}}) > 0) ? '':'*';
	foreach my $field_index (@{$self->{'bucket_group'}{$group_name}{'bucket_key_fields'}}) {
	    # Append field contents to the key.
	    # If field index is out of bounds,
	    # append nothing (an empty string)
	    $bucket_key .= defined($_[$field_index-1]) ?
				    $_[$field_index-1]:'';
	}
	# Bucket key must always be defined
	unless($bucket_key ne '') {
	    # ... do some debug logging
	    $self->api->logging('LOG_DEBUG', "Aggregator %s: invalid bucket key in bucket group %s",
					     $self->{'instance'},
					     $group_name);
	    next;
	}

	# If bucket doesn't exist yet ...
	unless(defined($instdata->{'bucket_group'}{$group_name}{'bucket'}{$bucket_key})) {
	    # ... initialize it
	    foreach my $bucket_field_name (keys %{$self->{'bucket_group'}{$group_name}{'bucket_field'}}) {
		#  ... zero bucket field
		$instdata->{'bucket_group'}{$group_name}{'bucket'}{$bucket_key}{'field'}{$bucket_field_name} = 0;
	    }
	    # ... schedule bucket for output
	    my $interval = $self->{'bucket_group'}{$group_name}{'bucket_output'}{'interval'};
	    $instdata->{'bucket_group'}{$group_name}{'output_queue'}->insert($bucket_key, $interval);
	    # ... do some debug logging
	    $self->api->logging('LOG_DEBUG', "Aggregator %s: creating bucket in bucket group %s (bucket key=\'%s\')",
					     $self->{'instance'},
					     $group_name,
					     $bucket_key);
	}

	# Format unique session key
	my $session_key = '';
	foreach my $field_index (@{$self->{'bucket_group'}{$group_name}{'session_key_fields'}}) {
	    # Append field contents to the key.
	    # If field index is out of bounds,
	    # append nothing (an empty string)
	    $session_key .= defined($_[$field_index-1]) ?
					$_[$field_index-1]:'';
	}

	# If session doesn't exist yet ...
	unless($instdata->{'bucket_group'}{$group_name}{'sessions'}->exists($session_key)) {
	    # ... create it and schedule it's expiration
	    my $timeout = $instdata->{'bucket_group'}{$group_name}{'session_idle_timeout'};
	    $instdata->{'bucket_group'}{$group_name}{'sessions'}->insert($session_key, $timeout);
	    #  ... do some debug logging
	    $self->api->logging('LOG_DEBUG', "Aggregator %s: creating session in bucket group %s (session key=\'%s\')",
					     $self->{'instance'},
					     $group_name,
					     $session_key);
	}

	# Aggregate each of records' specified
	# field values into this bucket
	foreach my $bucket_field_name (keys %{$self->{'bucket_group'}{$group_name}{'bucket_field'}}) {
	    # Input field index
	    my $field_index = $self->{'bucket_group'}{$group_name}{'bucket_field'}{$bucket_field_name}{'input_field'}{'index'} - 1;
	    # Current input field value
	    my $field_value = $_[$field_index];
	    # Field contents must be numeric
	    next unless(defined($field_value) && $field_value =~ /^\d+$/);
	    # Session default values:
	    #  - previous time is one second ago
	    my $prev_time = $current_time - 1;
	    #  - counter starts at zero
	    my $prev_field_value = 0;
	    #  - only one value
	    my $num_values = 1;
	    # If previous session data exist ...
	    if(defined($instdata->{'bucket_group'}{$group_name}{'records_cache'}{$session_key})) {
		# ... take previous counter field value
		$prev_field_value = $instdata->{'bucket_group'}{$group_name}{'records_cache'}{$session_key}{'data'}[$field_index];
		# .... take previous timestamp
		$prev_time = $instdata->{'bucket_group'}{$group_name}{'records_cache'}{$session_key}{'time'};
		# .... we have 2 values: current and previous
		$num_values++;
	    }
	    # If input field is volume type (absolute = always increasing until wrap),
	    # we must make it relative before aggregating it along with others
	    if($self->{'bucket_group'}{$group_name}{'bucket_field'}{$bucket_field_name}{'input_field'}{'type'} eq 'volume') {
		# Calculate delta between current and previous volume value
		# because we can only add deltas to aggregate counters.
		# If previous value is greater than the current one, counter
		# was either reset (for whatever reason) or it wrapped around
		# some maximum value. We can assume one or another. We have
		# 50:50 chance to be wrong. Since we can aggregate counters
		# from various architectures, we cannot assume what the max
		# value is, so we will assume that counter was reset and not
		# wrapped. In case we assumed wrong, error we introduce is
		# equal to (max_value - prev_value). In case we assumed
		# right, counter was reset so there is no previous value,
		# no delta. Thus, we can only add absolute value.
		$field_value -= ($field_value >= $prev_field_value) ? $prev_field_value:0;
	    # If input field is rate type (per-second units),
	    # we must convert it to relative volume value
	    # before aggregating it along with others
	    } elsif($self->{'bucket_group'}{$group_name}{'bucket_field'}{$bucket_field_name}{'input_field'}{'type'} eq 'rate') {
		# Calculate average rate between 2 field updates
		$field_value = int(($field_value + $prev_field_value) / $num_values);
		# Transform rate into volume by multiplying
		# rate with interval between 2 field updates.
		$field_value *= $current_time - $prev_time;
	    }
	    # Add value to this bucket's aggregate field
	    $instdata->{'bucket_group'}{$group_name}{'bucket'}{$bucket_key}{'field'}{$bucket_field_name} += $field_value;
	}

	# Mark the time of this update
	$instdata->{'bucket_group'}{$group_name}{'bucket'}{$bucket_key}{'timestamp'} = $current_time;

	# Cache processed record to be used in volume
	# delta calculation when the next record that
	# belongs to the same session arrives
	$instdata->{'bucket_group'}{$group_name}{'records_cache'}{$session_key}{'data'} = \@_;
	$instdata->{'bucket_group'}{$group_name}{'records_cache'}{$session_key}{'time'} = $current_time;
	# Refresh session to prevent removal
	$instdata->{'bucket_group'}{$group_name}{'sessions'}->refresh($session_key);

    }

    return;
}

##############################################################################################

sub init_bucket_groups($$) {
    my ($self, $instdata) = @_;
    my $event;

    foreach my $group_name (keys %{$self->{'bucket_group'}}) {
	# Every bucket group must specify record fields that
	# uniquely identify session records belongs to
	unless(defined($self->{'bucket_group'}{$group_name}{'session_key_fields'})) {
	    $self->api->logging('LOG_ERR', "Aggregator %s: bucket group %s requires fields that uniquely identify sessions that records belong to",
					   $self->{'instance'},
					   $group_name);
	    return undef;
	}
	# If bucket group doesn't already exists - initialize it
	unless(defined($instdata->{'bucket_group'}{$group_name}) &&
	       ref($instdata->{'bucket_group'}{$group_name}) eq 'HASH') {
	    # Create per group session index
	    my $sessions = api::util::lru->new(1);
	    unless(defined($sessions)) {
		$self->api->logging('LOG_ERR', "Aggregator %s: failed to initialize sessions index for bucket group %s",
					       $self->{'instance'},
					       $group_name);
		return undef;
	    }
	    # Keep initialized sessions index
	    $instdata->{'bucket_group'}{$group_name}{'sessions'} = $sessions;
	    # Create per group records cache
	    $instdata->{'bucket_group'}{$group_name}{'records_cache'} = {};
	    # Create per group bucket output queue
	    my $output_queue = api::util::lru->new(1);
	    unless(defined($output_queue)) {
		$self->api->logging('LOG_ERR', "Aggregator %s: failed to initialize output queue for bucket group %s",
					       $self->{'instance'},
					       $group_name);
		return undef;
	    }
	    # Keep initialized output queue
	    $instdata->{'bucket_group'}{$group_name}{'output_queue'} = $output_queue;
	    # Register event that will expire
	    # idle sessions for the group and
	    # either export one scheduled bucket
	    # or expire idle buckets on each run
	    $event = $self->api->create_loop_event('handler' => \&output_bucket_fields,
						   'args' => [ $self, $instdata, $group_name ]);
	# If bucket group exists, we only need to
	# change it's handler's input parameters
	} else {
	    # Modify loop event handler's  parameters
	    # because $self reference has changed
	    $event = $self->api->modify_loop_event($instdata->{'bucket_group'}{$group_name}{'worker'},
						   'args' => [ $self, $instdata, $group_name ]);
	}
	# Can't go on without worker
	unless(defined($event)) {
	    $self->api->logging('LOG_ERR', "Aggregator %s: failed to initialize worker for bucket group %s",
					   $self->{'instance'},
					   $group_name);
	    return undef;
	}
	# Keep reference to registered worker (loop event)
	$instdata->{'bucket_group'}{$group_name}{'worker'} = $event;
	# Maximum time a session can exist without change
	# (default: 8 hours)
	$instdata->{'bucket_group'}{$group_name}{'session_idle_timeout'} =
			defined($self->{'bucket_group'}{$group_name}{'session_idle_timeout'}) ?
					$self->{'bucket_group'}{$group_name}{'session_idle_timeout'}:28800;
    }

    return $instdata;
}

sub output_bucket_fields($$$$) {
    my ($event, $instance, $instdata, $group_name) = @_;

    # Get current timestamp
    my $current_time = time();

    # Expire sessions that haven't been 
    # updated in specified time
    while((my $session_key = $instdata->{'bucket_group'}{$group_name}{'sessions'}->expired)) {
        # Remove expired session's last record
	delete $instdata->{'bucket_group'}{$group_name}{'records_cache'}{$session_key};
	# Do some debug logging
	$instance->api->logging('LOG_DEBUG', "Aggregator %s: destroying idle session in bucket group %s (session key=\'%s\')",
					     $instance->{'instance'},
					     $group_name,
					     $session_key);
    }

    # Maximum time a bucket can stay without update
    # (default: 8 hours)
    my $idle_timeout = defined($instance->{'bucket_group'}{$group_name}{'bucket_idle_timeout'}) ?
					$instance->{'bucket_group'}{$group_name}{'bucket_idle_timeout'}:28800;

    # Get next scheduled bucket
    my $bucket_key = $instdata->{'bucket_group'}{$group_name}{'output_queue'}->expired;
    # If no scheduled buckets remain
    unless(defined($bucket_key)) {
	# ... put us to sleep for 1 sec
	$instance->api->delay_event($event, 1);
	# ... do some debug logging
	$instance->api->logging('LOG_DEBUG', "Aggregator %s: output queue in bucket group %s is empty: worker sleeping for 1 second",
					     $instance->{'instance'},
					     $group_name);
	# ... and stop here
	return;
    }

    # How long since the last update ?
    if($current_time - $instdata->{'bucket_group'}{$group_name}{'bucket'}{$bucket_key}{'timestamp'} >= $idle_timeout) {
	# Destroy idle bucket
	delete $instdata->{'bucket_group'}{$group_name}{'bucket'}{$bucket_key};
	# Do some debug logging
	$instance->api->logging('LOG_DEBUG', "Aggregator %s: destroying idle bucket in bucket group %s (bucket key=\'%s\')",
					     $instance->{'instance'},
					     $group_name,
					     $bucket_key);
	return;
    }

    my @record = ();

    # Format output record
    for(my $i = 0; $i < scalar(@{$instance->{'bucket_group'}{$group_name}{'bucket_output'}{'format'}}); $i++) {
	# Output record field name
	my $format_field_name = $instance->{'bucket_group'}{$group_name}{'bucket_output'}{'format'}[$i];
	if($format_field_name eq 'timestamp') {
	    # Timestamp field
	    push @record, int($current_time);
	} elsif($format_field_name eq 'group') {
	    # Bucket group name field
	    push @record, $group_name;
	} elsif($format_field_name eq 'key') {
	    # Bucket key field
	    push @record, $bucket_key;
	} else {
	    # Bucket field value
	    my $value = defined($instdata->{'bucket_group'}{$group_name}{'bucket'}{$bucket_key}{'field'}{$format_field_name}) ?
					$instdata->{'bucket_group'}{$group_name}{'bucket'}{$bucket_key}{'field'}{$format_field_name}:0;
	    # If bucket field is rate ...
	    if($instance->{'bucket_group'}{$group_name}{'bucket_field'}{$format_field_name}{'type'} eq 'rate') {
		# Value accumulated over bucket output interval
		# must be converted into per second value
		$value /= $instance->{'bucket_group'}{$group_name}{'bucket_output'}{'interval'};
	    }
	    # Add bucket field value
	    # to the output record
	    push @record, $value;
	}
    }

    # Do some debug logging
    $instance->api->logging('LOG_DEBUG', "Aggregator %s: bucket group %s exporting aggregate fields (bucket key=\'%s\', record=%s)",
					 $instance->{'instance'},
					 $group_name,
					 $bucket_key,
					 join(',', @record));

    # Check if bucket fields require maintenance
    foreach my $bucket_field_name (keys %{$instance->{'bucket_group'}{$group_name}{'bucket_field'}}) {
	# If bucket field is rate ...
	if($instance->{'bucket_group'}{$group_name}{'bucket_field'}{$bucket_field_name}{'type'} eq 'rate') {
	    # ...  we need to reset its value to 0
	    $instdata->{'bucket_group'}{$group_name}{'bucket'}{$bucket_key}{'field'}{$bucket_field_name} = 0;
	}
    }

    # Output interval in seconds (fractional)
    my $interval = $instance->{'bucket_group'}{$group_name}{'bucket_output'}{'interval'};

    # Output drift in seconds (fractional) - export counters
    # earlier or later than specified by output interval
    my $drift = $instance->{'bucket_group'}{$group_name}{'bucket_output'}{'drift'};
    if(defined($drift)) {
	# Drift is a random subsecond interval in range from -drift to +drift.
	# Purpose is to disperse counter exports over time to reduce bursts.
	$interval += (rand($drift) - rand($drift) + rand($drift) - rand($drift));
    }

    # Reschedule bucket for next field export
    $instdata->{'bucket_group'}{$group_name}{'output_queue'}->insert($bucket_key, $interval);

    # Deliver the record to the main process
    return (@record);
}

1;
