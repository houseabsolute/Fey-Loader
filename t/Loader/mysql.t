use strict;
use warnings;

use lib 't/lib';

use Fey::Test;
use Fey::Test::Loader;
use Fey::Test::mysql;

use Test::More tests => 119;

use Fey::Literal;
use Fey::Loader;


{
    my $loader = Fey::Loader->new( dbh => Fey::Test::SQLite->dbh() );

    my $schema1 = $loader->make_schema( name => 'Test' );
    my $schema2 = Fey::Test->mock_test_schema_with_fks();

    Fey::Test::Loader->compare_schemas
        ( $schema1, $schema2,
          { 'Message.message_id' =>
                { type   => 'INT',
                  length => 11,
                },
            'Message.message' =>
                { type   => 'VARCHAR',
                  length => 255,
                },
            'Message.quality' =>
                { type    => 'DECIMAL',
                  default => Fey::Literal->term('2.30'),
                },
            'Message.message_date' =>
                { type         => 'TIMESTAMP',
                  length       => 14,
                  precision    => 0, # gah, mysql is so weird
                  generic_type => 'datetime',
                  default      => Fey::Literal->term('CURRENT_TIMESTAMP'),
                  # mysql seems to always consider timestamp columns nullable
                  is_nullable  => 1,
                },
            'User.user_id' =>
                { type   => 'INT',
                  length => 11,
                },
            'User.username' =>
                { type    => 'TEXT',
                  default => Fey::Literal->string(''),
                },
            'User.email' =>
                { type   => 'TEXT',
                },
            'UserGroup.group_id' =>
                { type   => 'INT',
                  length => 11,
                },
            'UserGroup.user_id' =>
                { type   => 'INT',
                  length => 11,
                },
            'Group.group_id' =>
                { type   => 'INT',
                  length => 11,
                },
            'Group.name' =>
                { type    => 'TEXT',
                  default => Fey::Literal->string(''),
                },
            # DBD::mysql does not support foreign_key_info(), and
            # there's no good way of getting this from mysql
            # directly. Both SHOW CREATE TABLE and SHOW TABLE STATUS
            # include information about foreign keys, but in a rather
            # inconvenient form.
            skip_foreign_keys => 1,
          },
        );
}

{
    my $def = Fey::Loader::mysql->_default('NULL');
    isa_ok( $def, 'Fey::Literal::Null');
}
