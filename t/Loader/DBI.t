use strict;
use warnings;

use lib 't/lib';

use Fey::Test;
use Fey::Test::Loader;

use Test::More tests => 161;

use Fey::Loader;


{
    my $loader =
        do { local $SIG{__WARN__} =
                 sub { my @w = grep { ! /driver-specific/ } @_;
                       warn @w if @w; };
             Fey::Loader->new( dbh => Fey::Test->mock_dbh() ) };

    my $schema1 = $loader->make_schema();
    my $schema2 = Fey::Test->mock_test_schema_with_fks();

    Fey::Test::Loader->compare_schemas
        ( $schema1, $schema2,
          { 'Group.group_id'     => { is_auto_increment => 0 },
            'Message.message_id' => { is_auto_increment => 0 },
            'User.user_id'       => { is_auto_increment => 0 },
          },
        );
}

{
    my $def = Fey::Loader::DBI->_default('NULL');
    isa_ok( $def, 'Fey::Literal::Null');

    is( Fey::Loader::DBI->_default( q{'foo'} )->string(), 'foo',
        q{'foo' as default becomes string foo} );

    is( Fey::Loader::DBI->_default( q{"foo"} )->string(), 'foo',
        q{"foo" as default becomes string foo} );

    is( Fey::Loader::DBI->_default(42)->number(), 42,
        '42 as default becomes 42' );

    is( Fey::Loader::DBI->_default(42.42)->number(), 42.42,
        '42.42 as default becomes 42.42' );

    $def = Fey::Loader::DBI->_default('NOW');
    isa_ok( $def, 'Fey::Literal::Term' );
    is( $def->sql, 'NOW',
        'unquoted NOW as default becomes NOW as term' );
}
