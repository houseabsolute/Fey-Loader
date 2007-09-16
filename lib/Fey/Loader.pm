package Fey::Loader;

use strict;
use warnings;

our $VERSION = 0.01;

use Moose::Policy 'Fey::Policy';
use Moose;

has 'dbh' =>
    ( is       => 'ro',
      isa      => 'DBI::db',
      required => 1,
    );

no Moose;
__PACKAGE__->meta()->make_immutable();

use Fey::Loader::DBI;


sub new
{
    my $class = shift;
    my %p     = @_;

    my $self = $class->SUPER::new(%p);

    my $dbh = $self->dbh();
    my $driver = $dbh->{Driver}{Name};

    my $subclass = $self->_determine_subclass($driver);

    return $subclass->new(%p);
}

sub _determine_subclass
{
    my $self = shift;
    my $driver = shift;

    my $class = ref $self;

    my $subclass = $class . '::' . $driver;

    return $subclass if $subclass->can('new');

    eval "use $subclass";
    if ($@)
    {
        die $@ unless $@ =~ /Can't locate/;

        warn <<"EOF";

There is no driver-specific $class subclass for your driver ($driver)
... falling back to the base DBI implementation. This may or may not
work.

EOF

        return $class . '::' . 'DBI';
    }

    return $subclass;
}


1;

__END__
