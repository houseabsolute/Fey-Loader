use strict;
use warnings;

use Module::Build;

my $builder =
    Module::Build->new
        ( module_name        => 'Q::Loader',
          license            => 'perl',
          requires           => { 'Q::Core' => 0,
                                },
          build_requires     => { 'Test::More' => 0,
                                },
          auto_features      =>
          { mysql    => { description => 'mysql support',
                          requires    => { 'DBD::mysql' => 0 },
                        },
            SQLite   => { description => 'SQLite support',
                          requires    => { 'DBD::SQLite' => 0 },
                        },
            Postgres => { description => 'Postgres support',
                          requires    => { 'DBD::Pg' => 0 },
                        },
          },
          create_makefile_pl => 'passthrough',
          create_readme      => 1,
          sign               => 1,
        );

$builder->create_build_script();
