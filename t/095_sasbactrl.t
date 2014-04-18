# -*- perl -*-

# t/095_sasbactrl.t - check shell interface

BEGIN {
  eval {
    require Test::Command::Simple;
    Test::Command::Simple->import;
  };
}

use URI::file;

my $dsn = "testdb";
my $line;
my $cmd = "blib/script/sasbactrl";

use Test::More tests => 24;
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
    like(stdout, qr/\b0\.2_/, "version in result: ");
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
    like(stdout, qr/DATA_VERSION\s*:\s*2/, "correct DATA_VERSION in result: ");
};

# beacon
subtest "beacon w/o REVISIT" => sub {
    plan tests => 7;

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "beacon", "cgibase", "--preset", "FEED=http://localhost:1234/testbeacon.txt");
    is(rc >> 8, 0, "ask for 'beacon': ".stderr);
    is(stderr, "", "warnings on 'beacon': ".stderr);
    like(stdout, qr!VERSION\s*:\s*0\.1!, "correct VERSION in result: ");
    like(stdout, qr!FEED\s*:\s*http://localhost:1234/testbeacon.txt!, "correct FEED in result: ");
  };

# load
subtest "load" => sub {
    plan tests => 7;

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "load", "bar", "http://www.example.com/beacon2.txt", "t/beacon2.txt");
    is(rc >> 8, 0, "ask for 'load': ".stderr);
    is(stderr, "", "warnings on 'load': ".stderr);
    like(stdout, qr!NOTICE: New sequence \d+ for bar: processed 7 Records from 13 lines!, "correct sequence in result: ");
    like(stdout, qr!\(0 replaced, 5 new, 0 deleted, 2 duplicate, 0 nil, 0 invalid, 0 ignored\)!, "correct stats in result: ");
  };

subtest "update with blacklist" => sub {
    plan tests => 7;

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "update", "--force", "--blacklist", "^103", "bar", URI::file->new_abs("t/beacon2.txt"));
    is(rc >> 8, 0, "ask for 'update': ".stderr);
    is(stderr, "", "warnings on 'update': ".stderr);
    like(stdout, qr!NOTICE: New sequence \d+ for bar: processed 7 Records from 13 lines!, "correct sequence in result: ");
    like(stdout, qr!\(2 replaced, 0 new, 3 deleted, 2 duplicate, 0 nil, 0 invalid, 3 ignored\)!, "correct stats in result: ");
  };

subtest "update with whitelist" => sub {
    plan tests => 7;

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "update", "--force", "--whitelist", "^103", "bar");
    is(rc >> 8, 0, "ask for 'load': ".stderr);
    is(stderr, "", "warnings on 'load': ".stderr);
    like(stdout, qr!NOTICE: New sequence \d+ for bar: processed 7 Records from 13 lines!, "correct sequence in result: ");
    like(stdout, qr!\(0 replaced, 3 new, 2 deleted, 0 duplicate, 0 nil, 0 invalid, 4 ignored\)!, "correct stats in result: ");
  };

subtest "update" => sub {
    plan tests => 14;

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "update", "--force", "bar");
    is(rc >> 8, 0, "ask for 'update': ".stderr);
    is(stderr, "", "warnings on 'update': ".stderr);
    like(stdout, qr!NOTICE: New sequence \d+ for bar: processed 7 Records from 13 lines!, "correct sequence in result: ");
    like(stdout, qr!\(3 replaced, 2 new, 0 deleted, 2 duplicate, 0 nil, 0 invalid, 0 ignored\)!, "correct stats in result: ");

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "update", "--force", "sudoc");
    is(rc >> 8, 0, "ask for 'update': ".stderr);
    is(stderr, "", "warnings on 'update': ".stderr);
    like(stdout, qr!NOTICE: New sequence \d+ for sudoc: processed 2 Records from 11 lines!, "correct sequence in result: ");
    like(stdout, qr!\(2 replaced, 0 new, 0 deleted, 0 duplicate, 0 nil, 0 invalid, 0 ignored\)!, "correct stats in result: ");
  };

# idstat
subtest "idstat" => sub {
    plan tests => 12;
    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "idstat", 'ba%');
    is(rc >> 8, 0, "ask for 'idstat ba%': ".stderr);
    is(stderr, "", "warnings on 'idstat ba%': ".stderr);
    is(stdout, "5 identifiers\n", "correct # of identifiers in idstat: ");

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "idstat", "--distinct");
    is(rc >> 8, 0, "ask for 'idstat --distinct': ".stderr);
    is(stderr, "", "warnings on 'idstat --distinct': ".stderr);
    is(stdout, "7 identifiers\n", "correct # of identifiers in idstat --distinct: ");
  };

# idcounts
subtest "idcounts" => sub {
    plan tests => 13;

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "idcounts", "118%");
    is(rc >> 8, 0, "ask for 'idcounts 118%': ".stderr);
    is(stderr, "", "warnings on 'idcounts 118%': ".stderr);
    like(stdout, qr!118784226 2 0!, "missing '118784226 2 0'");
    unlike(stdout, qr!\b132464462\b!, "unwanted '132464462'");

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "idcounts", "--distinct");
    is(rc >> 8, 0, "ask for 'idcounts --distinct': ".stderr);
    is(stderr, "", "warnings on 'idcounts --distinct': ".stderr);
    like(stdout, qr!132464462 1 1!, "missing '132464462 1 1'");
  };

# idlist
subtest "idlist" => sub {
    plan tests => 14;

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "idlist");
    is(rc >> 8, 0, "ask for 'idlist': ".stderr);
    is(stderr, "", "warnings on 'idlist': ".stderr);
    like(stdout, qr!118784226\b.*\t13,,de.wikisource.org,http://toolserver.org/~apper/pd/person/pnd-redirect/ws/118784226!, "missing '118784226 ... 13,,de.wikisource.org,...'");
    like(stdout, qr!118624458\t!, "missing '118624458'");

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "idlist", "103%");
    is(rc >> 8, 0, "ask for 'idlist': ".stderr);
    is(stderr, "", "warnings on 'idlist': ".stderr);
    like(stdout, qr!103117741\b.*\t13,,!, "missing '103117741'");
    unlike(stdout, qr!\b118784226\b!, "unwanted '118784226'");
  };

# incidence
subtest "incidence" => sub {
    plan tests => 10;

    run_ok($cmd, "--dbroot", ".", "--dsn", $dsn, "incidence", "1%");
    is(rc >> 8, 0, "ask for 'incidence 1%': ".stderr);
    is(stderr, "", "warnings on 'incidence 1%': ".stderr);
    like(stdout, qr!118559796\|1\|bar!, "missing '118559796 bar'");
    like(stdout, qr!118624458\|1\|foo!, "missing '118624458 foo'");
    like(stdout, qr!118784226\|2\|bar foo!, "missing '118784226 bar|foo'");
    like(stdout, qr!103117741\|1\|bar\s!, "missing '103117741 bar'");
    unlike(stdout, qr![\s\|]([^\s\|]+)(?=\|)\S*\|\1!, "duplicate listing in result");
};
}

