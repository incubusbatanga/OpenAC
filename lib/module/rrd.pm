#
# module::rrd.pm
#
# Copyright (c) 2012 Marko Dinic <marko@yu.net>. All rights reserved.
# This program is free software; you can redistribute it and/or
# modify it under the same terms as Perl itself.
#

package module::rrd;

##############################################################################################

use strict;
use warnings;

##############################################################################################

use RRDs;
use File::Path qw(make_path);
use Config::ContextSensitive qw(:macros);

##############################################################################################

use api::module;

##############################################################################################

our @ISA = qw(api::module);

##############################################################################################

my $CONF_TEMPLATE = SECTION(
    DIRECTIVE('data_source', SKIP, REQUIRE(
	DIRECTIVE('type', MAP(FROM 'DS_TYPES', STORE(TO 'MODULE', KEY { '$SECTION' => { 'data_source' => { '$ARG[1]' => { 'type' => '$VALUE' } } } })), REQUIRE(
	    DIRECTIVE('field', ARG(CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION' => { 'data_source' => { '$ARG[1]' => { 'field' => '$VALUE' } } } })), ALLOW(
		DIRECTIVE('expr', ARG(CF_STRING, STORE(TO 'MODULE', KEY { '$SECTION' => { 'data_source' => { '$ARG[1]' => { 'expr' => '$VALUE' } } } })))
	    ))
	))
    ), OPTIONAL(
	    DIRECTIVE('min', ARG(CF_INTEGER, STORE(TO 'MODULE', KEY { '$SECTION' => { 'data_source' => { '$ARG[1]' => { 'min' => '$VALUE' } } } }))),
	    DIRECTIVE('max', ARG(CF_INTEGER, STORE(TO 'MODULE', KEY { '$SECTION' => { 'data_source' => { '$ARG[1]' => { 'max' => '$VALUE' } } } })))
    )),
    DIRECTIVE('time_source', REQUIRE(
	DIRECTIVE('field', ARG(CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION' => { 'timestamp_field' => '$VALUE' } })))
    )),
    DIRECTIVE('record_filter', OPTIONAL(
	DIRECTIVE('field', SKIP(CF_INTEGER|CF_POSITIVE|CF_NONZERO), REQUIRE(
	    DIRECTIVE('matching', ARG(CF_STRING, STORE(TO 'MODULE', KEY { '$SECTION' => { 'field' => { '$ARGR[-2]' => { 'match' => '$VALUE' } } } })))
	))
    )),
    DIRECTIVE('rrd_cache', ARG(CF_STRING, STORE(TO 'MODULE', KEY { '$SECTION' => { 'rrd_cache' => '$VALUE' } }))),
    DIRECTIVE('rrd_path', ARG(CF_PATH, STORE(TO 'MODULE', KEY { '$SECTION' => { 'rrd_path' => '$VALUE' } })), ALLOW(
	DIRECTIVE('/^(lowercase|uppercase)$/', ARG(CF_NONE, STORE(TO 'MODULE', KEY { '$SECTION' => { 'path_case' => '$DIRECTIVE' } }))),
    )),
    DIRECTIVE('rrd_timeout', ARG(CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION' => { 'rrd_timeout' => '$VALUE' } }), DEFAULT '5')),
    DIRECTIVE('delete_corrupt', ARG(CF_BOOLEAN, STORE(TO 'MODULE', KEY { '$SECTION' => { 'delete_corrupt' => '$VALUE' } }), DEFAULT 'no')),
    DIRECTIVE('update_step', ARG(CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION' => { 'update_interval' => '$VALUE' } }), DEFAULT '300')),
    DIRECTIVE('weekly_days_to_keep', ARG(CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION' => { 'week' => '$VALUE' } }), DEFAULT '7')),
    DIRECTIVE('monthly_weeks_to_keep', ARG(CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION' => { 'month' => '$VALUE' } }), DEFAULT '5')),
    DIRECTIVE('yearly_months_to_keep', ARG(CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION' => { 'year' => '$VALUE' } }), DEFAULT '12')),
    DIRECTIVE('lifetime_years_to_keep', ARG(CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION' => { 'life' => '$VALUE' } }), DEFAULT '5')),
    DIRECTIVE('weekly_sampling_interval', ARG(CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION' => { 'weekly_interval' => '$VALUE' } }), DEFAULT '300')),
    DIRECTIVE('monthly_sampling_interval', ARG(CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION' => { 'monthly_interval' => '$VALUE' } }), DEFAULT '1800')),
    DIRECTIVE('yearly_sampling_interval', ARG(CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION' => { 'yearly_interval' => '$VALUE' } }), DEFAULT '7200')),
    DIRECTIVE('liftime_sampling_interval', ARG(CF_INTEGER|CF_POSITIVE|CF_NONZERO, STORE(TO 'MODULE', KEY { '$SECTION' => { 'lifetime_interval' => '$VALUE' } }), DEFAULT '86400'))
);

##############################################################################################

our %DS_TYPES = (
    'counter'		=> 'COUNTER',
    'gauge'		=> 'GAUGE',
    'absolute'		=> 'ABSOLUTE',
    'derive'		=> 'DERIVE',
    'compute'		=> 'COMPUTE'
);

##############################################################################################

sub register() {
    # Return our configuration template
    return $CONF_TEMPLATE;
}

sub daemonize($) {
    my $self = shift;

    # Init instance private data
    my $instdata = {};

    # Set process() method timeout
    $self->set_process_timeout($self->{'rrd_timeout'});

    return $instdata;
}

sub initialize($$) {
    my ($self, $instdata) = @_;

    # Initialize parameters
    $self->reinitialize($instdata)
	or return undef;

    return $instdata;
}


sub reinitialize($$) {
    my ($self, $instdata) = @_;

    unless(defined($self->{'rrd_path'})) {
	$self->api->logging('LOG_ERR', "RRD backend %s: RRD path must be configured",
				       $self->{'instance'});
	return undef;
    }

    unless(defined($self->{'data_source'})) {
	$self->api->logging('LOG_ERR', "RRD backend %s: RRD must have at least one data source defined",
				       $self->{'instance'});
	return undef;
    }

    if(defined($self->{'field'}) && scalar(grep(/\D+/, (keys %{$self->{'field'}})))) {
	$self->api->logging('LOG_ERR', "RRD backend %s: record field indexes must be numeric",
				       $self->{'instance'});
	return undef;
    }

    # How many days to keep on weekly level
    $instdata->{'week'} = 86400 * $self->{'week'};
    # How many weeks to keep on monthly level
    $instdata->{'month'} = $instdata->{'week'} * $self->{'month'};
    # How many months to keep on yearly level
    $instdata->{'year'} = $instdata->{'month'} * $self->{'year'};
    # Howm many years to keep on lifetime level
    $instdata->{'life'} = $instdata->{'year'} * $self->{'life'};

    # How many PDPs aggregate into one CDP per interval ?
    $instdata->{'weekly_step'} = int($self->{'weekly_interval'} / $self->{'update_interval'});
    $instdata->{'monthly_step'} = int($self->{'monthly_interval'} / $self->{'update_interval'});
    $instdata->{'yearly_step'} = int($self->{'yearly_interval'} / $self->{'update_interval'});
    $instdata->{'lifetime_step'} = int($self->{'lifetime_interval'} / $self->{'update_interval'});

    # How many CDPs to archive per interval ?
    $instdata->{'weekly_steps'} = int($instdata->{'week'} / $self->{'weekly_interval'});
    $instdata->{'monthly_steps'} = int($instdata->{'month'} / $self->{'monthly_interval'});
    $instdata->{'yearly_steps'} = int($instdata->{'year'} / $self->{'yearly_interval'});
    $instdata->{'lifetime_steps'} = int($instdata->{'life'} / $self->{'lifetime_interval'});

    # Make sure record filter fields are sorted
    $instdata->{'fields'} = [sort keys %{$self->{'field'}}];

    return $instdata;
}

sub process($$;@) {
    my $self = shift;
    my $instdata = shift;

    # Do some debug logging
    $self->api->logging('LOG_DEBUG', "RRD backend %s: received record %s",
				     $self->{'instance'},
				     join(',',@_));

    my @backrefs = ();

    # Run fields through filter expressions to
    # select which records go into same RRDs
    foreach my $field (@{$instdata->{'fields'}}) {
	my $match_expr = $self->{'field'}{$field}{'match'};
	if(defined($match_expr) && $match_expr ne '') {
	    # At the same time, use capture groups to pick parts of
	    # data fields to construct dynamic path to RRD file.
	    my @cap = ($_[$field-1] =~ /($match_expr)/);
	    # Disregard records that do not match
	    unless(scalar(@cap) > 0) {
		# Do some debug logging
		$self->api->logging('LOG_DEBUG', "RRD backend %s: filter discarding record %s",
				     $self->{'instance'},
				     join(',',@_));
		return;
	    }
	    # Discard full expression match
	    shift @cap;
	    # Append captured strings to list of backreferences
	    splice(@backrefs, scalar(@backrefs), 0, @cap);
	}
    }

    # Format full path to the RRD file
    my $rrd_file = $self->rrd_filename(\@backrefs);
    unless(defined($rrd_file) && $rrd_file ne '') {
	# Do some debug logging
	$self->api->logging('LOG_ERR', "RRD backend %s: invalid RRD file name for record %s",
					 $self->{'instance'},
					 join(',',@_));
	return;
    }

    # If RRD file doesn't exist ...
    unless($self->is_rrd($rrd_file)) {
	# Check if it's directory exist
	my ($dir) = ($rrd_file =~ /^(.+)\/[^\/]+$/);
	# Regexp MUST produce directory path
	return unless defined($dir) && $dir ne '';
	# If directory path doesn't exist
	unless(-d $dir) {
	    make_path($dir);
	    # If creation failed, bail out
	    unless(-d $dir) {
		$self->api->logging('LOG_ERR', "RRD backend %s: failed to create directory %s",
					       $self->{'instance'},
					       $dir);
		return;
	    }
	}
	# Create RRD file
	$self->create_rrd($instdata, $rrd_file)
	    or return;
	# Do some debug logging
	$self->api->logging('LOG_DEBUG', "RRD backend %s: created RRD file %s",
					 $self->{'instance'},
					 $rrd_file);
    }

    # If record field was designated as timestamp, use it
    # for updates. Otherwise, let RRDTool decide ('N')
    my $timestamp = (defined($self->{'timestamp_field'}) &&
		     defined($_[$self->{'timestamp_field'}-1]) &&
		     ($_[$self->{'timestamp_field'}-1] =~ /^\d{10,}$/)) ?
			    $_[$self->{'timestamp_field'}-1]:'N';

    my @update_data = ();

    # Gather data sources for next RRD update
    foreach my $ds (keys %{$self->{'data_source'}}) {
	# Record field that is designated as RRD data source
	push @update_data, $_[$self->{'data_source'}{$ds}{'field'}-1];
    }

    # Update RRD
    if($self->update_rrd($rrd_file, $timestamp, @update_data)) {
	# Do some debug logging
	$self->api->logging('LOG_DEBUG', "RRD backend %s: updated RRD file %s",
					 $self->{'instance'},
					 $rrd_file);
    }

    return;
}

##############################################################################################

sub rrd_filename($$) {
    my $self = shift;
    my $backrefs = shift;

    # xlat backreference variables in the path
    my $rrd_path = $self->{'rrd_path'};
    for(my $i = 1; @{$backrefs}; $i++) {
	# Get contents of a capture group
	my $bref = shift @{$backrefs};
	return undef unless defined $bref;
	# Files/directories cannot be called '.'
	# That is reserved name for current dir
	if($bref eq '.') {
	    # Prepend underscore to
	    # make it a valid name
	    $bref = '_.';
	# Files/directories cannot be called '..' either
	# That is reserved name for parent dir
	} elsif($bref eq '..') {
	    # Prepend underscore to
	    # make it a valid name
	    $bref = '_..';
	}
	# Replace other unwanted chars with underscore
	$bref =~ s/[><=;:\&\#\`\'\"\s\?\!\/\\]/_/g;
	# Replace current index variable with
	# contents of current capture group
	$rrd_path =~ s/\$$i/$bref/g;
    }

    # This is the full path to the RRD file
    return defined($self->{'path_case'}) ?
			(($self->{'path_case'} eq 'lowercase') ?
				lc($rrd_path):uc($rrd_path)):$rrd_path;
}

sub is_rrd($$) {
    my ($self, $rrdfile) = @_;

    return (-f $rrdfile) ? 1:0;
}

sub create_rrd($$$) {
    my ($self, $instdata, $rrdfile) = @_;
    my @data_sources = ();

    # Format data source arguments
    foreach my $ds (keys %{$self->{'data_source'}}) {
	# Check for data source type
	my $type = $self->{'data_source'}{$ds}{'type'};
	# COMPUTE data sources take different
	# parameters from the rest
	if($type eq 'COMPUTE') {
	    # Get compute expression
	    my $expr = $self->{'data_source'}{$ds}{'expr'};
	    unless(defined($expr) && $expr ne '') {
		$self->api->logging('LOG_ERR', "RRD backend %s: failed to create RRD file %s: data source %s requires compute expression to be defined",
					       $self->{'instance'},
					       $rrdfile,
					       $ds);
		return 0;
	    }
	    # Format COMPUTE data source argument
	    push @data_sources,'DS:'.$ds.':'.$type.':'.$expr;
	# Here go other data source types: GAUGE, COUNTER, ABSOLUTE and DERIVE
	} else {
	    # Minimum data source value allowed
	    # before it is considered NaN
	    my $min = defined($self->{'data_source'}{$ds}{'min'}) ?
				$self->{'data_source'}{$ds}{'min'}:0;
	    # Maximum data source value allowed
	    # before it is considered NaN
	    my $max = defined($self->{'data_source'}{$ds}{'max'}) ?
				$self->{'data_source'}{$ds}{'max'}:'U';
	    # Format real data source argument
	    push @data_sources,'DS:'.$ds.':'.$type.':'.(2 * $self->{'update_interval'}).':'.$min.':'.$max;
	}
    }

    unless(scalar(@data_sources)) {
	$self->api->logging('LOG_ERR', "RRD backend %s: failed to create RRD %s: at least one data source must be defined",
				       $rrdfile,
				       $self->{'instance'});
	return 0;
    }

    RRDs::create($rrdfile,
		 '--step', $self->{'update_interval'},
		 '--start', time()-86400,
		 @data_sources,
		 'RRA:AVERAGE:0.5:'.$instdata->{'weekly_step'}.':'.$instdata->{'weekly_steps'},
		 'RRA:AVERAGE:0.5:'.$instdata->{'monthly_step'}.':'.$instdata->{'monthly_steps'},
		 'RRA:AVERAGE:0.5:'.$instdata->{'yearly_step'}.':'.$instdata->{'yearly_steps'},
		 'RRA:AVERAGE:0.5:'.$instdata->{'lifetime_step'}.':'.$instdata->{'lifetime_steps'},

		 'RRA:MIN:0.5:'.$instdata->{'weekly_step'}.':'.$instdata->{'weekly_steps'},
		 'RRA:MIN:0.5:'.$instdata->{'monthly_step'}.':'.$instdata->{'monthly_steps'},
		 'RRA:MIN:0.5:'.$instdata->{'yearly_step'}.':'.$instdata->{'yearly_steps'},
		 'RRA:MIN:0.5:'.$instdata->{'lifetime_step'}.':'.$instdata->{'lifetime_steps'},

		 'RRA:MAX:0.5:'.$instdata->{'weekly_step'}.':'.$instdata->{'weekly_steps'},
		 'RRA:MAX:0.5:'.$instdata->{'monthly_step'}.':'.$instdata->{'monthly_steps'},
		 'RRA:MAX:0.5:'.$instdata->{'yearly_step'}.':'.$instdata->{'yearly_steps'},
		 'RRA:MAX:0.5:'.$instdata->{'lifetime_step'}.':'.$instdata->{'lifetime_steps'},

		 'RRA:LAST:0.5:'.$instdata->{'weekly_step'}.':'.$instdata->{'weekly_steps'},
		 'RRA:LAST:0.5:'.$instdata->{'monthly_step'}.':'.$instdata->{'monthly_steps'},
		 'RRA:LAST:0.5:'.$instdata->{'yearly_step'}.':'.$instdata->{'yearly_steps'},
		 'RRA:LAST:0.5:'.$instdata->{'lifetime_step'}.':'.$instdata->{'lifetime_steps'});

    # Check for error
    my $error = RRDs::error;
    if(defined($error)) {
	$self->api->logging('LOG_ERR', "RRD backend %s: failed to create RRD file %s%s",
				       $self->{'instance'},
				       $rrdfile,
				       $error ne '' ? ': '.$error:'');
	return 0;
    }

    return 1;
}

sub update_rrd($$$;@) {
    my $self = shift;
    my $rrdfile = shift;
    my $timestamp = shift;

    # If rrdcached's address is specified
    # add it to the argument list
    my @rrdcmd = defined($self->{'rrd_cache'}) ?
		    ('--daemon', $self->{'rrd_cache'}):();

    # Format update command for rrdtool
    push @rrdcmd, $timestamp.':'.join(':', @_);

    # Some debug logging
    $self->api->logging('LOG_DEBUG', "RRD backend %s: updating RRD file %s with %s",
				     $self->{'instance'},
				     $rrdfile,
				     $rrdcmd[$#rrdcmd]);

    # The actual update
    RRDs::update($rrdfile, @rrdcmd);

    # Check for error
    my $error = RRDs::error;
    if(defined($error)) {
	$self->api->logging('LOG_ERR', "RRD backend %s: failed to update RRD file %s%s",
				       $self->{'instance'},
				       $rrdfile,
				       $error ne '' ? ': '.$error:'');

	# Should corrupt files be deleted
	# to allow us to re-create them ?
	if($self->{'delete_corrupt'}) {
	    # Check file attributes
	    my @stat = stat($rrdfile);
	    # Check if file is truncated
	    unless($stat[7] > 0) {
		$self->api->logging('LOG_ERR', "RRD backend %s: deleting zero length RRD file %s",
					       $self->{'instance'},
					       $rrdfile);
		# Remove truncated file
		unlink($rrdfile);
	    }
	}

	return 0;
    }

    return 1;
}

1;
