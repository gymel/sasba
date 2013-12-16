use Module::Build;
# See perldoc Module::Build for details of how this works

my $build = Module::Build->new(
	module_name => 'SeeAlso::Source::BeaconAggregator',
	requires => {
			  perl	=> '5.8.2',
			  Carp	=> 0,
		    File::Temp	=> 0,
			   CGI	=> '3.0',
		    HTTP::Date	=> '1.46',    # '5.0',
		 HTTP::Request	=> '1.40',    # '5.0',
			   LWP	=> '5.802',
		LWP::UserAgent	=> '2.031',
			   DBI	=> '1.57',
		   DBD::SQLite	=> '1.25',
	       SeeAlso::Server	=> '0.57',
	       SeeAlso::Source	=> '0.56',
	     SeeAlso::Response	=> '0.54',
	   SeeAlso::Identifier	=> '0.46',
		},
	build_requires => {
      SeeAlso::Identifier::GND	=> '0.54',
		    Test::More	=> '0.94',
		     URI::file	=> '1.0',
#         Test::Command::Simple	=> 0,
          Test::Command::Simple	=> 0,
		},
   configure_requires => {
		 Module::Build  => '0.35',
                },
	recommends => {
			   LWP	=> '5.827',
		  Getopt::Long	=> 0,
		    Pod::Usage	=> 0,
	       SeeAlso::Server	=> '0.58',
      SeeAlso::Identifier::GND	=> '0.55',
      SeeAlso::Identifier::PND	=> '0.57',
		},
#	script_files => [
#	'bin/sasbactrl',
#		],
    license => 'perl',
  meta_merge => {
      resources => {
# Spec 2
#	repository => {
#		url => 'https://github.com/gymel/sasba.git',
#		web => 'https://github.com/gymel/sasba',
#		type => 'git'},
#	bugtracker  => {
#		web => 'http://rt.cpan.org/Public/Dist/Display.html?Name=SeeAlso-Source-BeaconAggregator',
#		mailto => 'THB@cpan.org',
#	     },
	repository => 'https://github.com/gymel/sasba.git',
	bugtracker => 'http://rt.cpan.org/Public/Dist/Display.html?Name=SeeAlso-Source-BeaconAggregator',
	},
     },
	create_makefile_pl => 'traditional',
    );

$build->create_build_script();
