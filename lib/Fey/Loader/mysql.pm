package Fey::Loader::mysql;

use strict;
use warnings;

use Moose::Policy 'Fey::Policy';
use Moose;

extends 'Fey::Loader::DBI';

no Moose;
__PACKAGE__->meta()->make_immutable();

use DBD::mysql 4.004;

use Fey::Literal;


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
    else
    {
        return $default;
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


1;
