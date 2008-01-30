package Fey::Test::mysql;

use strict;
use warnings;

use Test::More;

BEGIN
{
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    unless ( eval { require DBD::mysql; 1 } )
    {
        plan skip_all => 'These tests require DBD::mysql';
    }

    unless ( $ENV{FEY_MAINTAINER_TEST_MYSQL} || -d '.svn' )
    {
        plan skip_all =>
            'These tests are only run if the FEY_MAINTAINER_TEST_MYSQL'
            . ' env var is true, or if being run from an SVN checkout dir.';
    }
}

use DBI;
use File::Spec;
use File::Temp ();


{
    my $DBH;
    sub dbh
    {
        my $class = shift;

        return $DBH if $DBH;

        my $dbh =
            DBI->connect
                ( 'dbi:mysql:', '', '', { PrintError => 0, RaiseError => 1 } );

        $dbh->func( 'dropdb', 'test_Fey', 'admin' );

        # The dropdb command apparently disconnects the handle.
        $dbh =
            DBI->connect
                ( 'dbi:mysql:', '', '', { PrintError => 0, RaiseError => 1 } );

        $dbh->func( 'createdb', 'test_Fey', 'admin' )
            or die $dbh->errstr();

        $dbh =
            DBI->connect
                ( 'dbi:mysql:test_Fey', '', '', { PrintError => 0, RaiseError => 1 } );

        $dbh->do( 'SET sql_mode = ANSI' );

        $class->_run_ddl($dbh);

        return $DBH = $dbh;
    }
}

sub _run_ddl
{
    my $class = shift;
    my $dbh   = shift;

    for my $ddl ( $class->_sql() )
    {
        $dbh->do($ddl);
    }
}

sub _sql
{
    return
        ( <<'EOF',
CREATE TABLE User (
    user_id   integer       not null  auto_increment,
    username  varchar(255)  unique not null,
    email     text          null,
    PRIMARY KEY (user_id)
) TYPE=INNODB
EOF
          <<'EOF',
CREATE TABLE "Group" (
    group_id   integer       not null  auto_increment,
    name       varchar(255)  not null,
    PRIMARY KEY (group_id),
    UNIQUE (name)
) TYPE=INNODB
EOF
          <<'EOF',
CREATE TABLE UserGroup (
    user_id   integer  not null,
    group_id  integer  not null,
    PRIMARY KEY (user_id, group_id),
    FOREIGN KEY (user_id)  REFERENCES User    (user_id),
    FOREIGN KEY (group_id) REFERENCES "Group" (group_id)
) TYPE=INNODB
EOF
          <<'EOF',
CREATE TABLE Message (
    message_id    integer       not null  auto_increment,
    quality       decimal(5,2)  not null  default 2.3,
    message       varchar(255)  not null  default 'Some message \'" text',
    message_date  timestamp     not null  default CURRENT_TIMESTAMP,
    parent_message_id  integer  null,
    user_id       integer       not null,
    PRIMARY KEY (message_id)
) TYPE=INNODB
EOF
          # This has to be done afterwards because the referenced
          # column doesn't exist until the create table is finished,
          # as far as mysql is concerned.
          <<'EOF',
ALTER TABLE Message
    ADD FOREIGN KEY (parent_message_id) REFERENCES Message (message_id)
EOF
          # I have no idea why this doesn't work when it's part of the
          # CREATE for Message
          <<'EOF',
ALTER TABLE Message
    ADD FOREIGN KEY (user_id) REFERENCES User (user_id)
EOF
          <<'EOF',
CREATE VIEW TestView
         AS SELECT user_id FROM User
EOF
        );
}


1;

__END__
