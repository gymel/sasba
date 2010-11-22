use Module::Build;
# See perldoc Module::Build for details of how this works

my $build = Module::Build->new
    ( module_name     => 'SeeAlso::Source::BeaconAggregator',
      requires => {
                 perl => '5.8.2',
                 Carp => 0,
           File::Temp => 0,
                  CGI => '3.0',
           HTTP::Date => '5.0',
        HTTP::Request => '5.0',
                  DBI => '1.6',
      SeeAlso::Server => '0.57',
  SeeAlso::Identifier => '0.46',
SeeAlso::Identifier::GND => '0.54',
                  },
      recommends => {
                 LWP => '5.827',
        Getopt::Long => 0,
          Pod::Usage => 0,
                  },
      license         => 'perl',
   create_makefile_pl => 'traditional',
    );

$build->create_build_script();
