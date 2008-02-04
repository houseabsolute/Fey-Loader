package Fey::Loader;

use strict;
use warnings;

our $VERSION = 0.02;

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

=head1 NAME

Fey::Loader - Load your schema defintion from a DBMS

=head1 SYNOPSIS

  my $loader = Fey::Loader->new( dbh => $dbh );

  my $schema = $loader->make_schema();

=head1 DESCRIPTION

C<Fey::Loader> takes a C<DBI> handle and uses it to construct a set of
Fey objects representing that schema. It will attempt to use an
appropriate DBMS subclass if one exists, but will fall back to using a
generic loader otherwise.

The generic loader simply uses the various schema information methods
specified by C<DBI>. This in turn depends on these methods being
implemented by the driver.

=head1 METHODS

This class provides the following methods:

=head2 Fey::Loader->new( dbh => $dbh )

Given a connected C<DBI> handle, this method returns a new loader. If
an appropriate subclass exists, it will be loaded and used. Otherwise,
it will warn and fall back to using L<Fey::Loader::DBI>.

=head1 AUTHOR

Dave Rolsky, <autarch@urth.org>

=head1 BUGS

Please report any bugs or feature requests to
C<bug-fey-loader@rt.cpan.org>, or through the web interface at
L<http://rt.cpan.org>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 COPYRIGHT & LICENSE

Copyright 2006-2008 Dave Rolsky, All Rights Reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut
