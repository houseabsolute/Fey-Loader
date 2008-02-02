package Fey::Loader::mysql;

use strict;
use warnings;

use Moose;

extends 'Fey::Loader::DBI';

use DBD::mysql 4.004;

use Fey::Literal;
use Scalar::Util qw( looks_like_number );

package # hide from PAUSE
    DBD::mysql::Fixup;

BEGIN
{
    unless ( defined &DBD::mysql::db::statistics_info )
    {
        *DBD::mysql::db::statistics_info = \&_statistics_info;
    }
}

sub _statistics_info {
  my ($dbh, $catalog, $schema, $table, $unique_only) = @_;
  $dbh->{mysql_server_prepare}||= 0;
  my $mysql_server_prepare_save= $dbh->{mysql_server_prepare};

  my $table_id = $dbh->quote_identifier($catalog, $schema, $table);

  my @names = qw(
      TABLE_CAT TABLE_SCHEM TABLE_NAME NON_UNIQUE INDEX_QUALIFIER
      INDEX_NAME TYPE ORDINAL_POSITION COLUMN_NAME COLLATION
      CARDINALITY PAGES FILTER_CONDITION
      );
  my %index_info;

  local $dbh->{FetchHashKeyName} = 'NAME_lc';
  my $desc_sth = $dbh->prepare("SHOW KEYS FROM $table_id");
  my $desc= $dbh->selectall_arrayref($desc_sth, { Columns=>{} });
  my $ordinal_pos = 0;

  for my $row (grep { $_->{key_name} ne 'PRIMARY'} @$desc)
  {
    next if $unique_only && $row->{non_unique};

    $index_info{ $row->{key_name} } = {
      TABLE_CAT        => $catalog,
      TABLE_SCHEM      => $schema,
      TABLE_NAME       => $table,
      NON_UNIQUE       => $row->{non_unique},
      INDEX_NAME       => $row->{key_name},
      TYPE             => lc $row->{index_type},
      ORDINAL_POSITION => $row->{seq_in_index},
      COLUMN_NAME      => $row->{column_name},
      COLLATION        => $row->{collation},
      CARDINALITY      => $row->{cardinality},
      mysql_nullable   => ( $row->{nullable} ? 1 : 0 ),
      mysql_comment    => $row->{comment},
    };
  }

  my $sponge = DBI->connect("DBI:Sponge:", '','')
    or 
     ($dbh->{mysql_server_prepare}= $mysql_server_prepare_save &&
      return $dbh->DBI::set_err($DBI::err, "DBI::Sponge: $DBI::errstr"));

  my $sth= $sponge->prepare("statistics_info $table", {
      rows          => [ map { [ @{$_}{@names} ] } values %index_info ],
      NUM_OF_FIELDS => scalar @names,
      NAME          => \@names,
      }) or 
       ($dbh->{mysql_server_prepare}= $mysql_server_prepare_save &&
        return $dbh->DBI::set_err($sponge->err(), $sponge->errstr()));

  $dbh->{mysql_server_prepare}= $mysql_server_prepare_save;

  return $sth;
}

package Fey::Loader::mysql;

sub _column_params
{
    my $self     = shift;
    my $table    = shift;
    my $col_info = shift;

    my %col = $self->SUPER::_column_params( $table, $col_info );

    # DBD::mysql adds the max length for some data types to the column
    # info, but we only care about user-specified lengths.
    #
    # Unfortunately, DBD::mysql itself adds a length to some types
    # (notably integer types) that isn't really useful, but it's
    # impossible to distinguish between a length specified by the user
    # and one specified by DBD::mysql.
    delete $col{length}
        if (    $col{type} =~ /(?:text|blob)$/i
             || $col{type} =~ /^(?:float|double)/i
             || $col{type} =~ /^(?:enum|set)/i
             || (    $col{type} =~ /^(?:date|time)/i
                  && lc $col{type} ne 'timestamp' )
           );

    delete $col{precision}
        if $col{type} =~ /date|time/o;

    delete $col{default}
        if (    exists $col{default}
             && $col_info->{COLUMN_DEF} eq ''
             && $col_info->{TYPE_NAME} =~ /int|float|double/i
           );

    return %col;
}

sub _is_auto_increment
{
    my $self     = shift;
    my $table    = shift;
    my $col_info = shift;

    return $col_info->{mysql_is_auto_increment} ? 1 : 0;
}

sub _default
{
    my $self     = shift;
    my $default  = shift;
    my $col_info = shift;

    if ( $default =~ /^NULL$/i )
    {
        return Fey::Literal::Null->new();
    }
    elsif ( $default =~ /^CURRENT_TIMESTAMP$/i )
    {
        return Fey::Literal::Term->new($default);
    }
    elsif ( looks_like_number($default) )
    {
        return Fey::Literal::Number->new($default);
    }
    else
    {
        return Fey::Literal::String->new($default);
    }
}

sub _fk_info_sth
{
    my $self = shift;
    my $name = shift;

    return
        $self->dbh()->foreign_key_info
            ( undef, $self->dbh()->{Name}, $name,
              undef, undef, undef,
            );
}

no Moose;
__PACKAGE__->meta()->make_immutable();

1;

=head1 NAME

Fey::Loader::mysql - Loader for Postgres schemas

=head1 SYNOPSIS

  my $loader = Fey::Loader->new( dbh => $dbh );

  my $schema = $loader->make_schema( name => $name );

=head1 DESCRIPTION

C<Fey::Loader::mysql> implements some MySQL-specific loader
behavior.

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
