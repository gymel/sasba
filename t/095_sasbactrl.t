# -*- perl -*-

# t/095_sasbactrl.t - check shell interface

BEGIN {
  eval {
    require Test::Command::Simple;
    Test::Command::Simple->import;
  };
}

my $dsn = "testdb";
my $line;
my $cmd = "blib/script/sasbactrl";

use Test::More tests => 16;
SKIP: {
    eval {
	require Test::Command::Simple;
      };
    skip "Test::Command::Simple not installed", 16 if $@;

    run_ok(2, $cmd, "");
    is(rc >> 8, 2, "Testing exit_status of empty call: ".(scalar stderr));

    run($cmd, "--dsn", $dsn);
    is(rc >> 8, 2, "Testing with dsn argument for existing: ".stderr);

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn);

subtest "noop" => sub {
    plan tests => 6;

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "noop");
    is(rc >> 8, 0, "ask for 'version': ".stderr);
    is(stderr, "", "warnings on 'version': ".stderr);
    is(stdout, "OK!\n", "noop result");
};

subtest "version" => sub {
    plan tests => 6;

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "version");
    is(rc >> 8, 0, "ask for 'version': ".stderr);
    is(stderr, "", "warnings on 'version': ".stderr);
    like(stdout, qr/\b0\.2_/, "version in result: ".stdout);
};

# Test schema_version
subtest "schema_version" => sub {
    plan tests => 6;

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "--pragma", 'schema_version', "noop");
    is(rc >> 8, 0, "ask for pragma 'schema_version': ".stderr);
    is(stderr, "", "warnings on query for 'schema_version': ".stderr);
    ($line) = split(/\r?\n/, stdout, 2);
    like($line, qr/^\d+$/, "query schema_version: ($line)");
};

# Test schema_version
subtest "user_version" => sub {
    plan tests => 18;

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "--pragma", 'user_version', "noop");
    is(rc >> 8, 0, "ask for pragma 'user_version': ".stderr);
    is(stderr, "", "warnings on query for 'user_version': ".stderr);
    ($line) = split(/\r?\n/, stdout, 2);
    like($line, qr/^\d*$/, "query user_version: ($line)");
    my $uvalue = ($line || 0) + 3;

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "--pragma", '"user_version='.$uvalue.'"', "noop");
    is(rc >> 8, 0, "set pragma 'user_version': ".stderr);
    is(stderr, "", "warnings on setting 'user_version': ".stderr);
    ($line) = split(/\r?\n/, stdout, 2);
    is($line, "OK!", "set user_version");

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "--pragma", 'user_version', "noop");
    is(rc >> 8, 0, "recheck pragma 'user_version': ".stderr);
    is(stderr, "", "warnings on query for 'user_version': ".stderr);
    ($line) = split(/\r?\n/, stdout, 2);
    is($line, $uvalue, "recheck user_version: ($line should be $uvalue)");
};

# Test cache_size
subtest "cache_size" => sub {
    plan tests => 24;

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "--pragma", 'cache_size', "noop");
    is(rc >> 8, 0, "ask for pragma 'cache_size': ".stderr);
    is(stderr, "", "warnings on query for 'cache_size': ".stderr);
    ($line) = split(/\r?\n/, stdout, 2);
    like($line, qr/^\d*$/, "query cache_size: ($line)");
    my $cvalue = int($line / 3);

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "--cache_size", "4000", "--pragma", 'cache_size', "noop");
    is(rc >> 8, 0, "ask for pragma 'cache_size' just set: ".stderr);
    is(stderr, "", "warnings on query for 'cache_size': ".stderr);
    ($line) = split(/\r?\n/, stdout, 2);
    is($line, 4000, "check cache_size just set: ($line should be 4000)");

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "--pragma", "'cache_size = $cvalue'", "noop");
    is(rc >> 8, 0, "set pragma 'cache_size': ".stderr);
    is(stderr, "", "warnings on setting 'cache_size': ".stderr);
    ($line) = split(/\r?\n/, stdout, 2);
    is($line, "OK!", "set cache_size");

# set again, then read: cache_size is not persistant...
    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "--pragma", "'cache_size = $cvalue'", "--pragma", "cache_size", "noop");
    is(rc >> 8, 0, "recheck pragma 'cache_size': ".stderr);
    is(stderr, "", "warnings on query for 'cache_size': ".stderr);
    ($line) = split(/\r?\n/, stdout, 2);
    is($line, $cvalue, "recheck cache_size: ($line should be $cvalue)");
};

# admin
subtest "admin" => sub {
    plan tests => 6;

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "admin");
    is(rc >> 8, 0, "ask for 'admin': ".stderr);
    is(stderr, "", "warnings on 'admin': ".stderr);
    like(stdout, qr/DATA_VERSION\s*:\s*2/, "correct DATA_VERSION in result: ".stdout);
};

# beacon
subtest "beacon w/o REVISIT" => sub {
    plan tests => 7;

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "beacon", "cgibase", "--preset", "FEED=http://localhost:1234/testbeacon.txt");
    is(rc >> 8, 0, "ask for 'beacon': ".stderr);
    is(stderr, "", "warnings on 'beacon': ".stderr);
    like(stdout, qr!VERSION\s*:\s*0\.1!, "correct VERSION in result: ".stdout);
    like(stdout, qr!FEED\s*:\s*http://localhost:1234/testbeacon.txt!, "correct FEED in result: ".stdout);
  };

};


