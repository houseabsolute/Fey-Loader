package Fey::Loader::Pg;

use strict;
use warnings;

use Moose::Policy 'Fey::Policy';
use Moose;

extends 'Fey::Loader::DBI';

no Moose;
__PACKAGE__->meta()->make_immutable();

use Fey::Literal;

use Scalar::Util qw( looks_like_number );

package # hide from PAUSE
    DBD::Pg::Fixup;

BEGIN
{
    unless ( defined &DBD::Pg::db::statistics_info )
    {
        *DBD::Pg::db::statistics_info = \&_statistics_info;
    }
}

# This is hacked up copy of primary_key_info from DBD::Pg 0.49,
# tweaked to fetch non-pk index info.
#
# It is far from a complete implementation of the method as defined by
# DBI. It ignores schemas and tablespaces, and doesn't get all of the
# statistics info defined by DBI.
sub _statistics_info
{
    my $dbh = shift;
    my ($catalog, $schema, $table, $unique_only, $quick) = @_;

    ## Catalog is ignored, but table is mandatory
    return undef unless defined $table and length $table;

    my $version = $dbh->{private_dbdpg}{version};
    my $whereclause = "AND c.relname = " . $dbh->quote($table);

    if ($unique_only) {
        $whereclause .= "\n\t\t\tAND i.indisunique IS TRUE";
    }

    my $key_sql = qq{
	SELECT
		  c.oid
		, quote_ident(c.relname)
		, quote_ident(c2.relname)
		, i.indkey
	FROM
		${DBD::Pg::dr::CATALOG}pg_class c
		JOIN ${DBD::Pg::dr::CATALOG}pg_index i ON (i.indrelid = c.oid)
		JOIN ${DBD::Pg::dr::CATALOG}pg_class c2 ON (c2.oid = i.indexrelid)
	WHERE
		i.indisprimary IS FALSE
	$whereclause
    };

    my $sth = $dbh->prepare($key_sql) or return undef;
    $sth->execute();
    my $info = $sth->fetchall_arrayref()->[0];
    return undef if ! defined $info;

    # Get the attribute information
    my $indkey = join ',', split /\s+/, $info->[3];
    my $sql = qq{
	SELECT a.attnum, ${DBD::Pg::dr::CATALOG}quote_ident(a.attname) AS colname,
		${DBD::Pg::dr::CATALOG}quote_ident(t.typname) AS typename
	FROM ${DBD::Pg::dr::CATALOG}pg_attribute a, ${DBD::Pg::dr::CATALOG}pg_type t
	WHERE a.attrelid = '$info->[0]'
	AND a.atttypid = t.oid
	AND attnum IN ($indkey);
    };
    $sth = $dbh->prepare($sql) or return undef;
    $sth->execute();

    my $attribs = $sth->fetchall_hashref('attnum');

    my $keyinfo = [];

    my $x=0;
    my @key_seq = split/\s+/, $info->[3];
    for (@key_seq) {
        # TABLE_CAT
        $keyinfo->[$x][0] = undef;
        # SCHEMA_NAME
        $keyinfo->[$x][1] = undef;
        # TABLE_NAME
        $keyinfo->[$x][2] = $info->[1];
        # COLUMN_NAME
        $keyinfo->[$x][3] = $attribs->{$_}{colname};
        # ORDINAL_POSITION
        $keyinfo->[$x][4] = $_;
        # INDEX_NAME
        $keyinfo->[$x][5] = $info->[2];

        $x++;
    }

    my @cols = (qw(TABLE_CAT TABLE_SCHEM TABLE_NAME COLUMN_NAME
                   ORDINAL_POSITION INDEX_NAME));

    return DBD::Pg::db::_prepare_from_data('foreign_key_info', $keyinfo, \@cols);
}

package Fey::Loader::Pg;

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


1;
