use strict;
use warnings;

use Module::Build;

my $builder =
    Module::Build->new
        ( module_name => 'Fey::Loader',
          license     => 'perl',
          requires => { 'DBI'                      => 0,
                        'Fey'                      => 0,
                        'Moose'                    => 0,
                        'MooseX::Params::Validate' => '0.08',
                        'Scalar::Util'             => 0,
                      },
          build_requires => { 'Fey::Test'  => 0.06,
                              'Test::More' => 0,
                            },
          auto_features => { mysql => { description => 'mysql support',
                                        requires    => { 'DBD::mysql' => 4.004 },
                                      },
                             SQLite => { description => 'SQLite support',
                                         requires    => { 'DBD::SQLite' => 1.14 },
                                       },
                             Postgres => { description => 'Postgres support',
                                           requires    => { 'DBD::Pg' => 2.0 },
                                         },
                           },
          recursive_test_files => 1,
          create_makefile_pl   => 'traditional',
          create_readme        => 1,
          sign                 => 1,
          meta_merge =>
              { resources =>
                    { homepage   => 'http://www.fey-project.org/',
                      bugtracker => 'http://rt.cpan.org/NoAuth/Bugs.html?Dist=Fey-Loader',
                      repository => 'http://hg.urth.org/hg/Fey-Loader',
                    },
              },
        );

$builder->create_build_script();
