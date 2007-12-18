package Fey::Loader;

use strict;
use warnings;

our $VERSION = 0.01;

use Fey::Loader::DBI;


sub new
{
    my $class = shift;
    my %p     = @_;

    my $dbh = $p{dbh};
    my $driver = $dbh->{Driver}{Name};

    my $subclass = $class->_determine_subclass($driver);

    return $subclass->new(%p);
}

sub _determine_subclass
{
    my $class = shift;
    my $driver = shift;

    my $subclass = $class . '::' . $driver;

    {
        # Shuts up UNIVERSAL::can
        no warnings;
        return $subclass if $subclass->can('new');
    }

    return $subclass if eval "use $subclass; 1;";

    die $@ unless $@ =~ /Can't locate/;

    warn <<"EOF";

There is no driver-specific $class subclass for your driver ($driver)
... falling back to the base DBI implementation. This may or may not
work.

EOF

    return $class . '::' . 'DBI';
}


1;

__END__
