package Fey::Loader::Pg;

use strict;
use warnings;

use Moose;

extends 'Fey::Loader::DBI';

use DBD::Pg 2.0;
use Fey::Literal;
use Scalar::Util qw( looks_like_number );


sub _schema_name { 'public' }

sub _column_params
{
    my $self     = shift;
    my $table    = shift;
    my $col_info = shift;

    my %col = $self->SUPER::_column_params( $table, $col_info );

    if ( defined $col{length} && $col{length} =~ /(\d+),(\d+)/ )
    {
        $col{length}    = $2;
        $col{precision} = $1;
    }

    delete $col{length}
        unless $col{precision} || $col{type} =~ /char/i;


    return %col
}

sub _is_auto_increment
{
    my $self     = shift;
    my $table    = shift;
    my $col_info = shift;

    return
        (    $col_info->{COLUMN_DEF}
          && $col_info->{COLUMN_DEF} =~ /^nextval\(/ ? 1 : 0
        );
}

sub _default
{
    my $self     = shift;
    my $default  = shift;
    my $col_info = shift;

    return if $default =~ /^nextval\(/;

    if ( $default =~ /^NULL$/i )
    {
        return Fey::Literal::Null->new();
    }
    elsif ( looks_like_number($default) )
    {
        return $default;
    }
    # string defaults come back like 'Foo'::character varying
    elsif ( $default =~ s/^\'(.+)\'::[^:]+$/$1/ )
    {
        $default =~ s/''/'/g;

        return Fey::Literal::String->new($default);
    }
    elsif ( $default =~ /\(.*\)/ )
    {
        return Fey::Literal::Term->new($default);
    }
}

no Moose;
__PACKAGE__->meta()->make_immutable();

1;

__END__

=head1 NAME

Fey::Loader::Pg - Loader for Postgres schemas

=head1 SYNOPSIS

  my $loader = Fey::Loader->new( dbh => $dbh );

  my $schema = $loader->make_schema( name => $name );

=head1 DESCRIPTION

C<Fey::Loader::Pg> implements some Postgres-specific loader behavior.

=head1 METHODS

This class provides the same public methods as L<Fey::Loader::DBI>.

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
