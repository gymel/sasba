package SeeAlso::Source::BeaconAggregator::Maintenance;
use strict;
use warnings;

BEGIN {
    use Exporter ();
    use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);
    $VERSION     = '0.2_86';
    @ISA         = qw(Exporter);
    #Give a hoot don't pollute, do not export more than needed by default
    @EXPORT      = qw();
    @EXPORT_OK   = qw();
    %EXPORT_TAGS = ();
}

use base ("SeeAlso::Source::BeaconAggregator");
use Carp;
use HTTP::Date;     # not perfect, but the module is commonly installed...
use HTTP::Request;
use LWP::UserAgent;
use File::Temp;

=head1 NAME

sasbactrl.pl - command line interface to SeeAlso::Source::BeaconAggregator and
               auxiliary classes

=head1 SYNOPSIS


=head1 DESCRIPTION

This Module allows a collection of BEACON files (cf. http://de.wikipedia.org/wiki/Wikipedia:BEACON)
to be used as SeeAlso::Source (probably in the context of an SeeAlso::Server application).
Therefore it implements the four methods documented in SeeAlso::Source

The BEACON files (lists of non-local identifiers of a certain type documenting the coverage of a given 
online database plus means for access) are imported by the methods provided by 
SeeAlso::Source::BeaconAggregator::Maintenance.pm, usually by employing the script sasbactrl.pl
as command line client.

Serving other formats than SeeAlso or providing a BEACON file with respect to this
SeeAlso service is achieved by using SeeAlso::Source::BeaconAggregator::Publisher.


=head1 USAGE

Use the C<new()> method inherited from C<SeeAlso::Source::BeaconAggregator> to
access an existing database or create a new one.


=head2 Database Methods

=head3 init( [ %options] )

Sets up and initializes the database structure for the object.
This has to be done once after creating a new database and after
upgrading this module.

Valid options include:

=over 8

=item verbose

=item prepareRedirs

=item identifierClass

=back


The I<repos> table contains as columns all valid beacon fields plus 
the following administrative fields which have to be prefixed with 
"_" in the interface:

=over 8

=item seqno

Sequence number: Is incremented on any successfull load

=item alias

Unique key: On update older seqences with the same alias are 
automatically discarded. Most methods take an alias as
argument thus obliterating the need to determine the sequence
number.

=item sort

optional sort key


=item uri

Overrides the #FEED header for updates

=item ruri

Real uri from which the last instance was loaded


=item ftime

Fetch time: Timestamp as to when this instance was loaded

Clear this or mtime to force automatic reload.

=item fstat

Short statistics line of last successful reload on update.


=item mtime

Modification time: Timestamp of the file / HTTP object from which this instance was loaded.
Identical to ftime if no timestamp is provided

Clear this or ftime to force automatic reload on update.


=item utime

Timestamp of last update attempt

=item ustat

Short status line of last update attempt.


=item counti

Identifier count

=item countu

Unique identifier count


=item admin

Just to store some remarks.

=back

The I<beacons> table stores the individual beacon entries from the input files.
Its columns are:

=over 8

=item hash

 Identifier. If a (subclass of) C<SeeAlso::Source::Identifier> instance is provided,
 this will be transformed by the C<hash()> method.

=item seqno

 Sequence number of the beacon file in the database

=item altid

 optional identifier from an alternative identifier system for use
 with ALTTARGET templates.

=item hits

 optional number of hits for this identifier in the given resource

=item info

 optional information text

=item link

 optional explicit URL   

=back


The I<osd> table contains C<key>, C<val> pairs for various metadata 
concerning the collection as such, notably the values needed for
the Open Search Description and the Header fields needed in case
of publishing a beacon file for this collection.

The I<admin> table stores (unique) C<key>, C<val> pairs for 
general persistent data. Currently the following keys are defined:

=over 8

=item DATA_VERSION

Integer version number to migrate database layout.

=item IDENTIFIER_CLASS

Name of the Identifier class to be used.

=item REDIRECTION_INDEX

Control creation of an additional index for the I<altid> column
(facialiates reverse lookups as needed for clustering).

=back


=cut

sub init {
  my ($self, %options) = @_;
  $options{'verbose'} = $self->{'verbose'} unless exists $options{'verbose'};

  my @fieldlist = SeeAlso::Source::BeaconAggregator->beaconfields();
  my @bf = map{ join(" ", @{[SeeAlso::Source::BeaconAggregator->beaconfields($_)]}[0..1]) } @fieldlist;
  my $hdl = $self->{dbh} or croak("no database handle?");

  local($") = ",\n";
  $hdl->do(<<"XxX"
CREATE TABLE IF NOT EXISTS repos (
    seqno INTEGER PRIMARY KEY AUTOINCREMENT,
    alias TEXT,
    sort TEXT,
    uri VARCHAR(512),
    ruri VARCHAR(512),
    mtime INTEGER,
    utime INTEGER,
    ftime INTEGER,
    counti INTEGER DEFAULT 0,
    countu INTEGER DEFAULT 0,
    fstat TEXT,
    ustat TEXT,
    admin VARCHAR(512),
    @bf
);
XxX
    ) or croak("Setup error: ".$hdl->errstr);

  $hdl->do("CREATE UNIQUE INDEX IF NOT EXISTS seqnos ON repos(seqno);") or croak("Setup error: ".$hdl->errstr);
  $hdl->do("CREATE INDEX IF NOT EXISTS aliases ON repos(alias);") or croak("Setup error: ".$hdl->errstr);

  $hdl->do(<<"XxX"
CREATE TABLE IF NOT EXISTS beacons (
    hash CHARACTER(64) NOT NULL,
    seqno INTEGER REFERENCES repos(seqno) ON DELETE CASCADE,
    altid TEXT,
    hits INTEGER,
    info VARCHAR(255),
    link VARCHAR(1024)    
);
XxX
    ) or croak("Setup error: ".$hdl->errstr);


# Faciliate lookups
  $hdl->do("CREATE INDEX IF NOT EXISTS lookup ON beacons(hash);") or croak("Setup error: ".$hdl->errstr);
# maintenance and enforce constraints
# (Problem: Dupes w/o altid but differing in link *and* info fields should be legitimate, too)
  $hdl->do("CREATE UNIQUE INDEX IF NOT EXISTS mntnce ON beacons(seqno, hash, altid);") or croak("Setup error: ".$hdl->errstr);

# foreign key on cascade does not work?

  $hdl->do(<<"XxX"
CREATE TRIGGER IF NOT EXISTS on_delete_seqno BEFORE DELETE ON repos FOR EACH ROW
 BEGIN
  DELETE FROM beacons WHERE seqno=OLD.seqno;
 END;
XxX
    ) or croak("Setup error: ".$hdl->errstr);

# OpenSearchDescription
  $hdl->do(<<"XxX"
CREATE TABLE IF NOT EXISTS osd (
    key CHAR(20) NOT NULL,
    val VARCHAR(1024)
);
XxX
    ) or croak("Setup error: ".$hdl->errstr);
  $hdl->do("CREATE INDEX IF NOT EXISTS OSDKeys ON osd(key);") or croak("Setup error: ".$hdl->errstr);

# Admin Stuff
  $hdl->do(<<"XxX"
CREATE TABLE IF NOT EXISTS admin (
    key CHAR(20) PRIMARY KEY NOT NULL,
    val VARCHAR(1024)
);
XxX
    ) or croak("Setup error: ".$hdl->errstr);

  $hdl->do("CREATE UNIQUE INDEX IF NOT EXISTS ADMKeys ON admin(key);") or croak("Setup error: ".$hdl->errstr);

  my $admref = $self->admhash();

  my $verkey = "DATA_VERSION";
  my $goalver = $SeeAlso::Source::BeaconAggregator::DATA_VERSION;
  my $dbver = $admref->{$verkey} || 0;
  if ( $dbver != $goalver ) {
      print "NOTICE: Database version $dbver: Upgrading to $goalver\n";
    # alter tables here
      if ( $dbver < 2 ) {
        #  my ($at, $type) = SeeAlso::Source::BeaconAggregator->beaconfields("COUNT");
        # $hdl->do("ALTER TABLE repos ADD COLUMN $at $type;");
        # ($at, $type) = SeeAlso::Source::BeaconAggregator->beaconfields("REMARK");
        # $hdl->do("ALTER TABLE repos ADD COLUMN $at $type;");
        };
    }
  elsif ( $options{'verbose'} ) {
      print "INFO: Database version $dbver is current\n"};

  unless ( $dbver == $goalver) {
      my $verh = $self->stmtHdl("INSERT OR REPLACE INTO admin VALUES (?, ?);", "update version statement");
      $verh->execute($verkey, $goalver)
              or croak("Could not execute update version statement: ".$verh->errstr);
    };

  unless ( exists $options{'identifierClass'} ) {
      $options{'identifierClass'} = $self->{'identifierClass'} if exists $self->{'identifierClass'};
   };

  my $ickey = "IDENTIFIER_CLASS";
  if ( (exists $options{identifierClass}) and (my $wanttype = ref($options{'identifierClass'})) ) {
      if ( (exists $self->{identifierClass}) && (ref($self->{identifierClass}) ne $wanttype) ) {
          croak("Cannot override identifierClass set on new()")};
      if ( my $oldtype = $admref->{$ickey} ) {
          croak ("Identifier mismatch: Cannot set to $wanttype since database already branded to $oldtype")
              unless($oldtype eq $wanttype);
        }
      else {
          print "fixing identifierClass as $wanttype\n" if $options{'verbose'};
          my $ichdl = $self->stmtHdl("INSERT INTO admin VALUES (?, ?);", "fix identifier class statement");
          $ichdl->execute($ickey, $wanttype)
                or croak("Could not execute fix identifier class statement: ".$ichdl->errstr);
          $self->{identifierClass} = $options{identifierClass};
        };
    }
  elsif ( (exists $options{identifierClass}) and (not $options{identifierClass}) ) {
      print "removing fixed identifierClass from admin table\n" if $options{'verbose'};
      my $ichdl = $self->stmtHdl("DELETE FROM admin WHERE key=?;", "identifier class statement");
      $ichdl->execute($ickey)
            or croak("Could not execute remove identifier class statement: ".$ichdl->errstr);
      delete $self->{identifierClass};
    };

  my $rikey = "REDIRECTION_INDEX";
  if ( exists $options{prepareRedirs} or exists $admref->{$rikey} ) {
      my $rihdl = $self->stmtHdl("INSERT OR REPLACE INTO admin VALUES (?, ?);", "fix redirection index statement");
      if ( $options{prepareRedirs} or ( $admref->{$rikey} and not exists $options{prepareRedirs} ) ) {
          print "creating redirection index\n" if $options{prepareRedirs} and $options{'verbose'};
          $hdl->do("CREATE INDEX IF NOT EXISTS redir ON beacons(altid,seqno);") or croak("Setup error: ".$hdl->errstr);
          $rihdl->execute($rikey, 1)
                or croak("Could not execute fix redirection index: ".$rihdl->errstr);
        }
      elsif ( not( $admref->{$rikey} and ($options{prepareRedirs} or (not exists $options{prepareRedirs})) ) ) {
          print "dropping redirection index\n" if $options{'verbose'};
          $hdl->do("DROP INDEX IF EXISTS redir;") or croak("Setup error: ".$hdl->errstr);
          $rihdl->execute($rikey, 0)
                or croak("Could not execute fix redirection index: ".$rihdl->errstr);
        };
#     $admref =  $self->admhash();
    }

  print "[ANALYZE ..." if $options{'verbose'};
  $hdl->do("ANALYZE;");
  print "]\n" if $options{'verbose'};
  return 1;    # o.k.
};


=head3 deflate()

Maintenance action: performs VACCUUM, REINDEX and ANALYZE on the database

=cut

sub deflate {
  my ($self) = @_;
  my $hdl = $self->{dbh} or croak("no handle?");
  print "VACUUM\n";
  $hdl->do("VACUUM") or croak("could not VACUUM: Abort");
  print "REINDEX\n";
  $hdl->do("REINDEX") or croak("could not REINDEX: Abort");
  print "ANALYZE\n";
  $hdl->do("ANALYZE;") or croak("could not ANALYZE: Abort");
  return 1;
}


=head2 Handling of beacon files

=head3 loadFile ( $file, $fields, %options ) 

Reads a physical beacon file and stores it with a new Sequence number in the
database.

Returns a triple:

 my ($seqno, $rec_ok, $message) = loadFile ( $file, $fields, %options ) 

$seqno is undef on error

$seqno and $rec_ok are zero with $message containing an explanation in case
of no action taken.

$seqno is an positive integer if something was loaded: The L<Sequence Number>
(internal unique identifier) for the representation of the beacon file in
the database.

=over 8

=item $file

File to read: Must be a beacon file

=item $fields

Hashref with additional meta and admin fields to store

=item Supported options: 

 verbose => (0|1)
 force => (0|1)   process unconditionally without timestamp comparison
 nostat => (0|1)  don't refresh global identifier counters

=back

If the file does not contain a minimal correct header (eg. is an empty file 
or an HTML error page accidentaly caught) no action is performed.

Otherwise, a fresh SeqNo (sequence number) is generated and meta and
BEACON-Lines are stored in the appropriate tables in the database.

If the _alias field is provided, existing database entries for this
Alias are updated, identifiers not accounted for any more are 
eventually discarded. 

=cut

sub loadFile {
  my ($self, $file, $fields, %options) = @_;
  $options{'verbose'} = $self->{'verbose'} unless exists $options{'verbose'};
  $options{'verbose'} ||= 0;

  if ( ! $file ) {
      croak("Missing file argument")}
  elsif ( ! -e $file ) {
      print "ERROR: no such file $file\n" && return undef}
  elsif ( ! -r _ ) {
      print "ERROR: no read permissions for $file\n" && return undef}
  elsif ( -z _ ) {
      print "WARNING: empty file $file\n";
      return (0,0, "empty file: Will not process");
    }
  my $mtime = (stat(_))[9];
  open(BKN, "<:utf8", $file) or (print "ERROR: cannot read $file\n", return undef);
  local($.) = 0;

  unless ( defined $self->{identifierClass} ) {
      my $package = $self->autoIdentifier();
      $options{'verbose'} && ref($package) && print "Assuming identifiers of type ".ref($package)."\n";
    };

  $fields = {} unless $fields;
  $fields->{'_ftime'} ||= time();
  $fields->{'_mtime'} ||= $mtime;
  delete $fields->{_uri} unless $fields->{_uri};
  delete $fields->{_alias} unless $fields->{_alias};
  my $autopurge = $fields->{_alias} || "";
  my $showme = $fields->{_alias} || $fields->{_uri} || $file;

  if ( $options{'verbose'} ) {
      printf("* Loading %s from URI %s\n", $fields->{_alias} || "<no alias>", $fields->{_uri} || "<direct file>");
      printf("* local input %s (%s)\n", $file, SeeAlso::Source::BeaconAggregator::tToISO($mtime));
    };

  my ($collno, $inserthandle, $replacehandle, $err, $format);
  my ($linecount, $headerseen, $oseq) = (0, 0, 0);
  my ($reccount, $recill, $recign, $recnil, $recupd, $recnew, $recdupl, $recdel) = (0, 0, 0, 0, 0, 0, 0, 0);
  local($_);
  lines:
  while ( <BKN> ) {
      s/[ \x0d\x0a]+$//;
      unless ( $linecount++ ) {
          if ( s/^\x{FEFF}// ) { # BOM-Character
            }
          elsif ( s/^\xef\xbb\xbf// ) { # BOM-Bytes
              print "ERROR: cannot cope with doubly UTF-8 encoded $file\n";
              return (undef, undef, "encoding trouble")};
          if ( /^\s*$/ ) {
              print "WARNING: Discarding blank line before beacon header [$showme l.$.]\n";
              next;
            };
        };
      if ( not defined $collno ) {      # $collno used as flag: "still in header"
          if ( /^#\s*([A-Z][\w-]*):\s*(.*)$/ ) {
              $headerseen++;
              my ($field, $data) = ($1, $2);
              $field =~ s/^DATE$/TIMESTAMP/ && print "WARNING: corrected DATE to TIMESTAMP in Beacon-Header [$showme l.$.]\n";
              $data =~ s/\s+$//;
              next if $data =~ /^\s*$/;
              if ( SeeAlso::Source::BeaconAggregator->beaconfields($field) ) {
                  if ( $fields->{$field} ) {
                      print "WARNING: Skipping already set $field [$showme l.$.]\n"}
                  else {
                      $fields->{$field} = $data}
                }
              else {
                  print "WARNING: Ignoring unknown $field [$data] [$showme l.$.]\n";
                };
            }
          elsif ( /^(#[^:\s]+)/ ) {
              print "WARNING: Discarding unparseable line >$1...< in beacon header context [$showme l.$.]\n"}
          elsif ( /^\s*$/ ) {
              print "NOTICE: Discarding blank line in beacon header context [$showme l.$.]\n" if $options{'verbose'}}
          elsif ( ! $headerseen ) {
              print "ERROR: no header fields [$showme l.$.]\n";
              return (0, 0, "no header fields: Will not proceed");
            }
          else {
              ($collno, $err, $format, $inserthandle, $replacehandle, $oseq) = $self->processbeaconheader($fields, %options);
              unless ( $collno ) {
                  print "ERROR: metadata error [$showme l.$.]\n";
                  return (0, 0, "metadata error: $err");
                };
              $self->{dbh}->{AutoCommit} = 0;
              $linecount --;
              redo lines;
            }
        }
      else {
         s/^\s+//; s/\s+$//;
         my ($id, $altid, @rest);
         ($id, @rest) = split(/\s*\|\s*/, $_, 4);
         ($id, $altid) = split(/\s*=\s*/, $id, 2) if $id;
         $id || ($recnil++, next);

         if ( $options{'filter'} ) {
             ($id, $altid) = &{$options{'filter'}}($id, $altid, @rest);
             unless ( $id ) {
                 $recign ++;
                 unless ( ++$reccount % 10000 ) {
                     $self->{dbh}->{AutoCommit} = 1;
                     print "$reccount\n" if $options{'verbose'};
                     $self->{dbh}->{AutoCommit} = 0;
                   };
                 next lines;
               };
           };
         $altid ||= "";

         my($hits, $info, $link);
         if ( @rest && ($rest[$#rest] =~ m!^\S+://\S+$!) ) {
             $link = pop @rest}
         elsif ( defined $rest[2] ) {
             print "WARNING: unparseable link content >$rest[2]< [$showme l.$.]"};

         if ( @rest && ($rest[0] =~ /^\d*$/) ) {
             $hits = shift @rest;
                                    # really disregard hits with explicit 0?
             $info = shift @rest || "";
           }
         elsif ( defined $rest[1] ) {
             $hits = "";
             shift @rest;
             $info = shift @rest;
           }
         elsif ( defined $rest[0] ) {
             $hits = "";
             $info = shift @rest;
           };
         if ( @rest ) {
             print "WARNING: unparseable content >$_< [$showme l.$.]"};

         unless ( $link ) {
             if ( ($format =~ /\bhasTARGET\b/) ) {   # ok
               }
             elsif ( $altid && ($format =~ /\baltTARGET\b/) ) {   # also ok
               }
             elsif ( $format =~ /\bnoTARGET\b/ ) {
                 print "NOTICE: discarding >$id<".(defined $hits ? " ($hits)" : "")." without link [$showme l.$.]\n" if $options{'verbose'} > 1;
                 $recill++;
                 next lines;
               }
             else {
                 print "WARNING: discarding >$id<".(defined $hits ? " ($hits)" : "")." without link [$showme l.$.] (assertion failed)\n";
                 $recill++;
                 next lines;
               }
           };

         if ( $format !~ /\baltTARGET\b/ ) {            # Allow certain duplicates (force disambiguisation)
             $altid ||= $info || $link}

         $hits = "" unless defined $hits;
         ($hits =~ /^0+/) && ($recnil++, next);          # Explizit "0" => raus
         $hits = 0 if $hits eq "";
         $altid ||= "";
         my $hash;
         if ( defined $self->{identifierClass} ) {
             $self->{identifierClass}->value($id);
             unless ( $self->{identifierClass}->valid ) {
                 print "NOTICE: invalid identifier >$id< ($hits) [$showme l.$.]\n" if $options{'verbose'};
                 $recill++;
                 next lines;
               };
             $hash = $self->{identifierClass}->hash();
           }
         else {
             $hash = $id};
         my $did;
         if ( $replacehandle && ($did = $replacehandle->execute($hits, $info, $link, $hash, $altid)) ) { # UPDATE OR FAIL old record
             if ( $replacehandle->err ) {
carp("update in trouble: $replacehandle->errstring [$showme l.$.]");
                 $recdupl++;
               }
             elsif ( $did eq "0E0" ) {  # not found, try insert
                 $did = $inserthandle->execute($hash, $altid, $hits, $info, $link);
                 if ( $did eq "0E0" ) {
                     $recdupl++;
                     if ( $altid ) {
                         print "INFO: did not insert duplicate Id >$id< = >$altid< ($hits) [$showme l.$.]\n" if $options{'verbose'}}
                     else {
                         print "INFO: did not insert duplicate Id >$id< ($hits) [$showme l.$.]\n" if $options{'verbose'} > 1};
                   }
                 else {
                     $recnew++};
               }
             else {
                 $recupd++};
           }
         elsif ( $did = $inserthandle->execute($hash, $altid, $hits, $info, $link) ) { # INSERT OR IGNORE new record
             if ( $did eq "0E0" ) {
                 $recdupl++;
                 print "INFO: did not insert duplicate Id $id ($hits) [$showme l.$.]\n" if $options{'verbose'} > 1;
               }
             else {
                 $recnew++};
           }
         elsif ( $inserthandle->errstr =~ /constraint/ ) {
             $recdupl++;
             print "INFO: duplicate Id $id ($hits): not inserting [$showme l.$.]\n" if $options{'verbose'} > 1;
           }
         else {
             croak("Could not insert: ($id, $hits, $info, $link): ".$inserthandle->errstr)};

         unless ( ++$reccount % 10000 ) {
             $self->{dbh}->{AutoCommit} = 1;
             print "$reccount\n" if $options{'verbose'};
             $self->{dbh}->{AutoCommit} = 0;
           };
        }
    };
  if ( not defined $collno ) {
      if ( $headerseen ) {
          ($collno, $err, $format, $inserthandle, $replacehandle, $oseq) = $self->processbeaconheader($fields, %options);
          if ( $collno ) {
              print "WARNING: no idn content in file [$showme l.$.]\n"}
          else {
              print "ERROR: metadata error [$showme l.$.]\n";
              return (0,0, "metadata error: $err");
            };
        }
      elsif ( $. ) {
          print "ERROR: no header fields [$showme l.$.]\n";
          return (0, 0, "no header fields: Will not proceed");
        }
      else {
          print "WARNING: empty file [$showme]\n";
          return (0,0, "empty file");
        };
    }
  $self->{dbh}->{AutoCommit} = 1;

  if ( $autopurge ) {
      $self->{dbh}->{AutoCommit} = 0;
      if ( $oseq ) {
          my ($bcdelh, $bcdelexpl) = $self->stmtHdl("DELETE FROM beacons WHERE seqno==?");
          $self->stmtExplain($bcdelexpl, $oseq) if $ENV{'DBI_PROFILE'};
          my $rows = $bcdelh->execute($oseq) or croak("Could not execute >".$bcdelh->{Statement}."<: ".$bcdelh->errstr);
          $self->{dbh}->{AutoCommit} = 1;
          printf("INFO: Purged %s surplus identifiers from old sequence %u\n", $rows, $oseq) if $options{'verbose'};
          $rows = "0" if $rows eq "0E0";
          $recdel += $rows;
        };

      $self->{dbh}->{AutoCommit} = 0;
      my ($rpdelh, $rpdelexpl) = $self->stmtHdl("DELETE FROM repos WHERE (alias=?) AND (seqno<?);");
      $self->stmtExplain($rpdelexpl, $autopurge, $collno) if $ENV{'DBI_PROFILE'};
      my $rows = $rpdelh->execute($autopurge, $collno) or croak("Could not execute >".$rpdelh->{Statement}."<: ".$rpdelh->errstr);
      $self->{dbh}->{AutoCommit} = 1;
      $rows = "0" if $rows eq "0E0";
      printf("INFO: %u old sequences discarded\n", $rows) if $options{'verbose'};
    }

  printf "NOTICE: New sequence %u for %s: processed %u Records from %u lines\n",
                          $collno, $autopurge || "???", $reccount,  $linecount;
  my $statline = sprintf "%u replaced, %u new, %u deleted, %u duplicate, %u nil, %u invalid, %u ignored",
                          $recupd,     $recnew, $recdel,   $recdupl,     $recnil, $recill,   $recign;
  print "       ($statline)\n";

  my $recok = $recupd + $recnew;
  my $numchg = ($recnew or $recdel) ? 1 : 0;

#  my $ct1hdl = $self->stmtHdl("SELECT COUNT(*) FROM beacons WHERE seqno==? LIMIT 1;");
#  $ct1hdl->execute($collno) or croak("could not execute live count: ".$ct1hdl->errstr);
#  my $ct1ref = $ct1hdl->fetchrow_arrayref();
#  my $counti = $ct1ref->[0] || 0;

# my $ct2hdl = $self->stmtHdl("SELECT COUNT(DISTINCT hash) FROM beacons WHERE seqno==?");
# using subquery to trick SQLite into using indices
#  my $ct2hdl = $self->stmtHdl("SELECT COUNT(*) FROM (SELECT DISTINCT hash FROM beacons WHERE seqno==?) LIMIT 1;");
#  $ct2hdl->execute($collno) or croak("could not execute live count: ".$ct2hdl->errstr);
#  my $ct2ref = $ct2hdl->fetchrow_arrayref();
#  my $countu = $ct2ref->[0] || 0;

# combined query turned out as not as efficient
# my $ct0hdl = $self->stmtHdl("SELECT COUNT(*), COUNT(DISTINCT hash) FROM beacons WHERE seqno==? LIMIT 1;");
# $ct0hdl->execute($collno) or croak("could not execute live count: ".$ct0hdl->errstr);
# my $ct0ref = $ct0hdl->fetchrow_arrayref();
# my ($counti, $countu) = ($ct0ref->[0] || 0, $ct0ref->[1] || 0);

  my ($updh, $updexpl) = $self->stmtHdl(<<"XxX");
UPDATE OR FAIL repos SET counti=?,countu=?,fstat=?,utime=?,ustat=?,sort=? WHERE seqno==?;
XxX

  my $counti = $self->idStat($collno, 'distinct' => 0) || 0;
  printf("WARNING: expected %u valid records, counted %u\n", $recok, $counti) if $recok != $counti;
  unless ( $numchg ) {
      $fields->{'_counti'} ||= 0;
      printf("WARNING: expected unchanged number %u valid records, counted %u\n", $fields->{'_counti'}, $counti) if $fields->{'_counti'} != $counti;
    };

  my $sort = $fields->{'_sort'} || "";
  my $countu = $numchg ? ( $self->idStat($collno, 'distinct' => 1) || 0 )
                       : ( $fields->{'_countu'} || $self->idStat($collno, 'distinct' => 1) || 0 );
  $self->stmtExplain($updexpl, $counti, $countu, $statline, time(), "successfully loaded", $sort, $collno) if $ENV{'DBI_PROFILE'};
  $updh->execute($counti, $countu, $statline, time(), "successfully loaded", $sort, $collno)
      or croak("Could not execute >".$updh->{Statement}."<: ".$updh->errstr);
  close(BKN);

  if ( $numchg or $options{'force'} ) {
#      if ( $options{'force'} ) {
#          print "[ANALYZE ..." if $options{'verbose'};
#          $self->{dbh}->do("ANALYZE;");
#          print "]\n" if $options{'verbose'};
#        };

      if ( $options{'nostat'} ) {   # invalidate since they might have changed
          $self->admin('gcounti', undef);
          $self->admin('gcountu', undef);
        }
      else {
          $self->admin('gcounti', $self->idStat(undef, 'distinct' => 0) || 0);
          $self->admin('gcountu', $self->idStat(undef, 'distinct' => 1) || 0);
        }
    };

  return ($collno, $recok, undef);
}


=head4 processbeaconheader($self, $fieldref, [ %options] )

Internal subroutine used by C<loadFile()>.

=over 8

=item $fieldref

Hash with raw fields.

=item Supported options: 

 verbose => (0|1)

Show seqnos of old instances which are met by the alias

=back


=cut

sub processbeaconheader {
  my ($self, $fieldref, %options) = @_;
  my $osq = 0;
  my @carp;

  if ( my $alias = $fieldref->{_alias} ) {
      my $stampfield = SeeAlso::Source::BeaconAggregator->beaconfields("TIMESTAMP");
      my ($listh, $listexpl) = $self->stmtHdl("SELECT seqno, $stampfield, mtime, counti, countu FROM repos WHERE alias=?;");
      $self->stmtExplain($listexpl, $alias) if $ENV{'DBI_PROFILE'};
      $listh->execute($alias) or croak("Could not execute >".$listh->{Statement}."<: ".$listh->errstr);
      my ($rowcnt, $ocounti, $ocountu);
      while ( my($row) = $listh->fetchrow_arrayref ) {
          last unless defined $row;
          $rowcnt ++;
          ($ocounti, $ocountu) = ($row->[3], $row->[4]);
          if ( $options{'verbose'} ) {
              print "* Old Instances for $alias:\n" unless $osq;
              $osq = $row->[0];
              print "+\t#$osq ", SeeAlso::Source::BeaconAggregator::tToISO($row->[1] || $row->[2]), " (", $row->[3] || "???", ")\n";
            }
          else {
              $osq = $row->[0]};
        }
      if ( $rowcnt && ($rowcnt == 1) ) {
          $fieldref->{_counti} ||= $ocounti if $ocounti;
          $fieldref->{_countu} ||= $ocountu if $ocountu;
        }
    };

  my $format = "";
  if ( $fieldref->{'FORMAT'} && $self->{accept}->{'FORMAT'} ) {
      if (  $fieldref->{'FORMAT'} =~ $self->{accept}->{'FORMAT'} ) {
          $format = $fieldref->{'FORMAT'}}
      else {
          push(@carp, "ERROR: only FORMAT '".$self->{accept}->{'FORMAT'}."' are supported, this is ".$fieldref->{'FORMAT'})}
    }
  elsif ( $fieldref->{'FORMAT'} ) {
      $format = $fieldref->{'FORMAT'}}
  else {
      push(@carp, "ERROR: header field #FORMAT is mandatory")};

  unless ( $fieldref->{'VERSION'} ) {
       $fieldref->{'VERSION'} ||= "0.1";
       push(@carp, "NOTICE: added header field #VERSION as '".$fieldref->{'VERSION'}."'");
    };
  if ( $self->{accept}->{'VERSION'} ) {
      ($fieldref->{'VERSION'} =~ $self->{accept}->{'VERSION'})
       || push(@carp, "ERROR: only VERSION '".$self->{accept}->{'VERSION'}."' is supported, this is ".$fieldref->{'VERSION'});
    };

  if ( $fieldref->{'ALTTARGET'} ) {
      $fieldref->{'ALTTARGET'} = "" unless defined $fieldref->{'ALTTARGET'};
      my $parsed = hDecode($fieldref, 'ALTTARGET');
      if ( $parsed && ($parsed =~ /(^|[^%])(%.)*%\d\$s/) ) {
          $fieldref->{'ALTTARGET'} = $parsed;
          $format =~ s/\s*-altTARGET//;
          $format .= " -altTARGET";
          ($parsed =~ /(^|[^%])(%.)*%2\$s/) or 
              push(@carp, "WARNING: header field #ALTTARGET should contain placeholder {ALTID}");
        }
      elsif ( $parsed ) {
          push(@carp, "ERROR: header field #ALTTARGET must contain placeholder {ALTID} (or {ID})");
          delete $fieldref->{'ALTTARGET'};
        }
      else {
          push(@carp, "ERROR: could not parse header field #ALTTARGET: '".$fieldref->{'ALTTARGET'}."'");
          delete $fieldref->{'ALTTARGET'};
        }
    };

  if ( $fieldref->{'IMGTARGET'} ) {
      $fieldref->{'IMGTARGET'} = "" unless defined $fieldref->{'IMGTARGET'};
      my $parsed = hDecode($fieldref, 'IMGTARGET');
      if ( $parsed && ($parsed =~ /(^|[^%])(%.)*%\d\$s/) ) {
          $fieldref->{'IMGTARGET'} = $parsed;
          $format =~ s/\s*-imgTARGET//;
          $format .= " -imgTARGET";
        }
      elsif ( $parsed ) {
          push(@carp, "WARNING: header field #IMGTARGET should contain placeholders {ID} or {ALTID}")}
      else {
          push(@carp, "ERROR: could not parse header field #IMGTARGET: '".$fieldref->{'IMGTARGET'}."'");
          delete $fieldref->{'IMGTARGET'};
        }
    };

  if ( exists $fieldref->{'TARGET'} ) {
      $fieldref->{'TARGET'} = "" unless defined $fieldref->{'TARGET'};
      my $parsed = hDecode($fieldref, 'TARGET');
      if ( $parsed && ($parsed =~ /(^|[^%])(%.)*%1\$s/) && ($parsed !~ /(^|[^%])(%.)*%[2-9]\$s/) ) {
          $fieldref->{'TARGET'} = $parsed;
          $format .= " -hasTARGET";
        }
      elsif ( $parsed ) {
          push(@carp, "ERROR: header field #TARGET must contain placeholder {ID} only");
          delete $fieldref->{'TARGET'};
        }
      else {
          push(@carp, "ERROR: could not parse header field #TARGET: '".$fieldref->{'TARGET'}."'");
          delete $fieldref->{'TARGET'};
        }
    }
  elsif ( $format =~ /^BEACON/ ) {
      push(@carp, "WARNING: header field #TARGET not set: ALL beacon lines will have to provide their link by other means!");
      $format =~ s/\s*-noTARGET//;
      $format .= " -noTARGET";
    }
  else {
      push(@carp, "ERROR: header field #TARGET is mandatory")};


  $fieldref->{'MESSAGE'} = hDecode($fieldref, 'MESSAGE') if $fieldref->{'MESSAGE'};

  if ( $fieldref->{'TIMESTAMP'} ) {
      if ( my $parsed = hDecode($fieldref, 'TIMESTAMP') ) {
          printf("* %-30s %s\n", "Beacon Timestamp:", hEncode($parsed, 'TIMESTAMP')) if $options{'verbose'};
          $fieldref->{'TIMESTAMP'} = $parsed;
        }
      else {           # unparseable => use current
          push(@carp, "WARNING: cannot parse TIMESTAMP '".$fieldref->{'TIMESTAMP'}."', using current time");
          $fieldref->{'TIMESTAMP'} = $^T;
        };
    }
  else {
#     $fieldref->{'TIMESTAMP'} = $fieldref->{'_mtime'} || $^T;
      push(@carp, "NOTICE: no header field #TIMESTAMP detected");
    };

  if ( $fieldref->{'REVISIT'} ) {
      if ( my $parsed = hDecode($fieldref, 'REVISIT') ) {
          if ( $parsed < $^T ) {
              printf("* %-30s %s [%s]\n", "STALE Revisit hint parsed as", hEncode($parsed, 'REVISIT'), $fieldref->{'REVISIT'})}       #  if $options{'verbose'}
          else {
              printf("* %-30s %s\n", "Revisit hint parsed as", hEncode($parsed, 'REVISIT')) if $options{'verbose'}};
          $fieldref->{'REVISIT'} = $parsed;
        }
      else {           # unparseable => discard
          push(@carp, "WARNING: cannot parse #REVISIT '".$fieldref->{'REVISIT'}."', discarding");
          delete $fieldref->{'REVISIT'};
        };
    }
  else {
      push(@carp, "INFO: no header field #REVISIT detected");
    };

  my $cancontinue = 1;
  my $err = "";
  foreach ( @carp ) {
      print "$_\n";
      if ( s/^ERROR: // ) {
          $cancontinue = 0;
          $err .= " | " if $err;
          $err .= $_;
        };
    }
  unless ( $cancontinue or $options{'ignore-header-errors'} ) {
      print "CRITICAL: Aborting because of Header Errors\n";
      return (undef, $err, $format);
    };

  $fieldref->{'_uri'} ||= $fieldref->{'FEED'};
  delete $fieldref->{'_uri'} unless $fieldref->{'_uri'};

  $fieldref->{'_alias'} ||= $fieldref->{'FEED'} || $fieldref->{'TARGET'};

  my (@fn, @fd);
  while ( my ($key, $val) = each %$fieldref ) {
      next unless defined $val;
      my $dbkey = "";
      if ( $dbkey = SeeAlso::Source::BeaconAggregator->beaconfields($key) ) {
          push(@fn, $dbkey)}
      elsif ( $key =~ /_(\w+)$/ ) {
          push(@fn, $1)}
      else {
          next};
      my $myval = $val;
      unless ( $myval =~ /^\d+$/ ) {
          $myval =~ s/'/''/g;
          $myval = "'".$myval."'";
        };
      push(@fd, $myval);
    };
  local($") = ",\n";
  my ($sth, $sthexpl) = $self->stmtHdl(<<"XxX");
INSERT INTO repos ( seqno, @fn ) VALUES ( NULL, @fd );
XxX
  $self->stmtExplain($sthexpl) if $ENV{'DBI_PROFILE'};
  $sth->execute() or croak("Could not execute >".$sth->{Statement}."<:".$sth->errstr);
  my $collno = $self->{dbh}->last_insert_id("", "", "", "");

  my $rhandle;
  if ( $osq ) {
      $rhandle = $self->stmtHdl(<<"XxX");
UPDATE OR FAIL beacons SET seqno=$collno, hits=?, info=?, link=? WHERE hash=? AND seqno==$osq AND altid=?;
XxX
    };
  my $ihandle = $self->stmtHdl(<<"XxX");
INSERT OR IGNORE INTO beacons ( hash, seqno, altid, hits, info, link ) VALUES (?, $collno, ?, ?, ?, ?);
XxX
  return ($collno, "", $format, $ihandle, $rhandle, $osq);
}




my ($lwpcarp817, $lwpcarp827);

=head3 update ($sq_or_alias, $params, %options)

Loads a beacon file into the database, possibly replacing a previous instance.

Some magic is employed to autoconvert ISO-8859-1 or doubly UTF-8 encoded files
back to UTF-8.

Returns undef, if something goes wrong, or the file was not modified since,
otherwise returns a pair (new seqence number, number of lines imported).


=over 8


=item $sq_or_alias

Sequence number or alias: Used to determine an existing instance.


=item $params

Hashref, containing

  agent => LWP::UserAgent to use
  _uri  => Feed URL to load from

=item %options

Hash, propagated to C<loadFile()>

 verbose => (0|1)
 force => (0|1)   process unconditionally without timestamp comparison
 nostat => (0|1)  don't refresh global identifier counters

=back

Incorporates a new beacon source from a URI in the database or updates an existing one. 
For HTTP URIs care is taken not to reload an unmodified BEACON feed (unless the 'force'
option is provided). 

If the feed appears to be newer than the previously loaded version it is fetched, 
some UTF-8 adjustments are performed if necessary, then it is stored to a temporary file
and from there finally processed by the C<loadFile()> method above.

The URI to load is determined by the following order of precedence:

=over 8

=item 1

_uri Option

=item 2

admin field uri stored in the database

=item 3

meta field #FEED taken from the database

=back

Typical use is with an alias, not with a sequence number:

 $db->update('whatever');

Can be used to initially load beacon files from URIs:

 $db->update("new_alias", {_uri => $file_uri} );

=cut

sub update {
  my ($self, $sq_or_alias, $params, %options) = @_;
  $params = {} unless $params;
  $options{'verbose'} = $self->{'verbose'} unless exists $options{'verbose'};

  my $ua = $params->{'agent'};
  unless ( $ua ) {
      require LWP::UserAgent;
      $ua = LWP::UserAgent->new(agent => "SA-S-BeaconAggregator ",      # end with space to get default agent appended
                            env_proxy => 1,
                              timeout => 300,
                               ) or croak("cannot create UserAgent");
    };

  my ($cond, @cval) = SeeAlso::Source::BeaconAggregator::mkConstraint($sq_or_alias);
  my $alias = ($sq_or_alias =~ /^\d+$/) ? "" : $sq_or_alias;
  my $feedname = SeeAlso::Source::BeaconAggregator->beaconfields("FEED");
  my ($ssth, $ssthexpl) = $self->stmtHdl(<<"XxX");
SELECT seqno, uri, alias, $feedname, ftime, mtime, sort FROM repos $cond;
XxX
  $self->stmtExplain($ssthexpl, @cval) if $ENV{'DBI_PROFILE'};
  $ssth->execute(@cval) or croak("Could not execute >".$ssth->{Statement}."<: ".$ssth->errstr);
  croak("Select old instance error: ".$ssth->errstr) if $ssth->err;
  my $aryref = $ssth->fetchrow_arrayref;
  my ($osq, $ouri, $oalias, $feed, $fetchtime, $modtime, $osort) = $aryref ? @$aryref : ();

  my $uri = $params->{'_uri'} || $ouri || $feed;
  croak("Cannot update $sq_or_alias: URI not given nor determinable from previous content") unless $uri;
  $uri =~ s/\s$//;
  $alias ||= $oalias || "";

  print "Requesting $uri\n" if $options{'verbose'};
  my $rq = HTTP::Request->new('GET', $uri, ['Accept' => 'text/*']) or croak("could not construct request from $uri");
  if ( $fetchtime && $modtime  && !$options{'force'} ) {   # allow force-reload by deleting _ftime or _mtime
      printf("  %-30s %s\n", "Old instance stamped", scalar localtime($modtime)) if $options{'verbose'};
      $rq->header('If-Modified-Since', HTTP::Date::time2str($modtime));
    };
  if ( $rq->can("accept_decodable") ) {  # LWP 5.817 and newer
      $rq->accept_decodable}
  else {
      carp("please upgrade to LWP >= 5.817 for compression negotiation") if $options{'verbose'} && (!$lwpcarp817++)};

  my $response = $ua->request($rq);   # Well, we hoggishly slurp everything into memory,
                                      # however explicit decompression of an already dumped result would be PITA
  my $nuri = ($response->request)->uri;
  print "NOTICE: Differing result URI: $nuri\n" if $uri ne $nuri;
  if ( $response->is_success ) {
      print $osq ? "INFO: refreshing $alias sq $osq from $uri\n"
                 : "INFO: importing previously unseen $alias from $uri\n";
      my $charset;
      if ( $response->can("content_charset") ) {    # LWP 5.827 and above
          $charset = $response->content_charset;
          print "DEBUG: Content charset is $charset\n" if $charset && $options{'verbose'};
        }
      else {
          carp("please upgrade to LWP >= 5.827 for better detection of content_charset") if $options{'verbose'} && (!$lwpcarp827++)};
      $charset ||= "UTF-8";

      my $lm = $response->last_modified;
      printf("  %-30s %s\n", "Last modified", scalar localtime($lm)) if $lm && $options{'verbose'};
      $lm ||= $^T;

      my $vt = $response->fresh_until(h_min => 1800, h_max => 30 * 86400);
      printf("  %-30s %s\n", "Should be valid until", scalar localtime($vt)) if $vt && $options{'verbose'};
      $vt ||= 0;

      # temporary file for dumped contents
      my ($tmpfh, $tmpfile) = File::Temp::tempfile("BeaconAggregator-XXXXXXXX", SUFFIX => ".txt", TMPDIR => 1) or croak("Could not acquire temporary file for storage");
      my $contref;   # reference to content buffer
      if ( ! $response->content_is_text ) {
          my $ct = $response->content_type;
          print "WARNING: Response content is $ct, not text/*\n";
          if ( my $ce = $response->content_encoding ) {
              print "NOTICE: Response is also Content-encoded: $ce\n"}
          my $ctt = join("|", $response->decodable());
          if ( $ct =~ s!^(.+\/)?($ctt)$!$2! ) {
      # yes: decode anyway since it could be a gzip-encoded .txt.gz file!
              my $cr = $response->decoded_content( raise_error => 1, ref => 1);   # method exists since LWP 5.802 (2004-11-30)
              $response->remove_content_headers;
              my $newresp = HTTP::Response->new($response->code, $response->message, $response->headers);
              $newresp->content_type("text/plain; charset: $charset");
              $newresp->content_encoding($ct);
              $newresp->content_ref($cr);
              $response = $newresp;
            }
        };
      $contref = $response->decoded_content( raise_error => 1, ref => 1);   # method exists since LWP 5.802 (2004-11-30)

      if ( $$contref =~ /^\x{FFEF}/ ) {          # properly encoded BOM => put Characters to file
          binmode($tmpfh, ":utf8");
          print "INFO: properly encoded BOM detected: Groked UTF8\n"; # if $options{'verbose'};
        }
      elsif ( $$contref =~ s/^\xef\xbb\xbf// ) {   # BOM Bytes => put Bytes to file, re-read as UTF-8
          print "INFO: Byte coded BOM detected: trying to restitute character semantics\n"; # if $options{'verbose'};
          print "INFO: Length is ", length($$contref), " ", (utf8::is_utf8($$contref) ? "characters" : "bytes"), "\n";
          binmode($tmpfh, ":bytes");
          utf_deduplicate($contref) && binmode($tmpfh, ":utf8");
        }
      elsif ( utf8::is_utf8($$contref) ) {       # already Upgraded strings should be written as utf-8
          print "INFO: UTF8-ness already established\n" if $options{'verbose'};
          binmode($tmpfh, ":utf8");
          utf_deduplicate($contref);             # but don't trust it (older LWP with file URLs, ...)            
        }
      elsif ( utf8::decode($$contref) ) {        # everything in character semantics now
          print "INFO: Could decode bytes to UTF8-characters\n" if $options{'verbose'};
          binmode($tmpfh, ":utf8");
        }
      else {                                     # leave it alone
          print "WARNING: No clue about character encoding: Assume ISO 8859-1\n"; # if $options{'verbose'};
          binmode($tmpfh, ":utf8");
        };
      print $tmpfh $$contref;
      close($tmpfh);
      # early cleanup since everything might be huge....
      $contref = $response = undef;

      my ($collno, $count, $statref) = $self->loadFile($tmpfile, {_alias => $alias, _uri => $uri, _ruri => $nuri, _mtime => $lm, _sort => $osort}, %options);
      if ( ! $collno && $osq ) {
          my ($usth, $usthexpl) = $self->stmtHdl(<<"XxX");
UPDATE OR FAIL repos SET utime=?,ustat=? WHERE seqno==?;
XxX
          $self->stmtExplain($usthexpl, time(), ($statref ? "load error: $statref" : "internal error"), $osq) if $ENV{'DBI_PROFILE'};
          $usth->execute(time(), ($statref ? "load error: $statref" : "internal error"), $osq)
               or croak("Could not execute >".$usth->{Statement}."<: ".$usth->errstr);
        };

      unlink($tmpfile) if -f $tmpfile;
      return $collno ? ($collno, $count) : undef;
    }
  elsif ( $response->code == 304 ) {
      print "INFO: $alias not modified since ".HTTP::Date::time2str($modtime)."\n";
      my $vt = $response->fresh_until(h_min => 1800, h_max => 6 * 86400);
      printf("  %-30s %s\n", "Will not try again before", scalar localtime($vt)) if $options{'verbose'};
      my ($usth, $usthexpl) = $self->stmtHdl(<<"XxX");
UPDATE OR FAIL repos SET utime=?,ustat=?,ruri=? WHERE seqno==?;
XxX
      $self->stmtExplain($usthexpl, time(), $response->status_line, $nuri, $osq) if $ENV{'DBI_PROFILE'};
      $usth->execute(time(), $response->status_line, $nuri, $osq)
          or croak("Could not execute >".$usth->{Statement}."<: ".$usth->errstr);
      return undef;
    }
  else {
      print "WARNING: No access to $uri for $alias [".$response->status_line."]\n";
      print $response->headers_as_string, "\n";
      return undef unless $osq;
      my ($usth, $usthexpl) = $self->stmtHdl(<<"XxX");
UPDATE OR FAIL repos SET utime=?,ustat=?,ruri=? WHERE seqno==?;
XxX
      $self->stmtExplain($usthexpl, time(), $response->status_line, $nuri, $osq) if $ENV{'DBI_PROFILE'};
      $usth->execute(time(), $response->status_line, $nuri, $osq)
          or croak("Could not execute >".$usth->{Statement}."<: ".$usth->errstr);
      return undef;
    };
}



sub utf_deduplicate {
  my ($success, $stringref) = (0, @_);
  if ( utf8::downgrade($$stringref, 1) ) {   # 1 = FAIL_OK
      my $prevlength = length($$stringref);
      print "INFO: Downgrade was possible, length now $prevlength ", (utf8::is_utf8($$stringref) ? "characters" : "bytes"), "\n";
      while ( utf8::decode($$stringref) ) {
          $success ++;
          my $newlength = length($$stringref);
          print "DEBUG: Reassembling as UTF-8 succeeded, length now $newlength ", (utf8::is_utf8($$stringref) ? "characters" : "bytes"), "\n";
          last if $newlength == $prevlength;
          $prevlength = $newlength;
#         last unless utf8::downgrade($$stringref, 1);
        }
    }
  else {
      print "WARNING: no downgrade possible, proceed with byte semantics";
    };
  return $success;
}

=head3 unload ( [ $seqno_or_alias, %options ] ) 

Deletes the sequence(s).

=over 8

=item $seqno_or_alias

 numeric sequence number, Alias or SQL pattern.

=item Supported options: 

 force => (0|1)

Needed to purge the complete database ($seqno_or_alias empty) or to purge
more than one sequence ($seqno_or_alias yields more than one seqno).

=back


=cut

sub unload {
  my ($self, $seqno_or_alias, %options) = @_;
  $options{'verbose'} = $self->{'verbose'} unless exists $options{'verbose'};

  my @seqnos = ();
  if ( $seqno_or_alias && ($seqno_or_alias =~ /^\d+$/) ) {
      @seqnos = ($seqno_or_alias)}
  elsif ( $seqno_or_alias || $options{'force'} ) {
      @seqnos = $self->Seqnos('_alias', $seqno_or_alias ? ($seqno_or_alias) : ());
      unless ( @seqnos ) {
          carp("no Seqnos selected by $seqno_or_alias");
          return 0;
        };
      unless ( $options{'force'} or (@seqnos == 1) ) {
          carp("Use --force to purge more than one sequence (@seqnos)");
          return 0;
        };
    }
  else {
      carp("Use --force to purge the complete database");
      return 0;
    };

  if ( $options{'force'} ) {
      my ($sth, $sthexpl) = $self->stmtHdl(<<"XxX");
DELETE FROM beacons WHERE seqno==?;
XxX
      foreach my $seqno ( @seqnos ) {
          $self->stmtExplain($sthexpl, $seqno_or_alias) if $ENV{'DBI_PROFILE'};
          my $rows = $sth->execute($seqno_or_alias) or croak("Could not execute >".$sth->{Statement}."<: ".$sth->errstr);
          print "INFO: $rows forced for $seqno\n" if $options{'verbose'};
        };
    };

  my ($cond, @cval) = SeeAlso::Source::BeaconAggregator::mkConstraint($seqno_or_alias);
  my ($sth, $sthexpl) = $self->stmtHdl(<<"XxX");
DELETE FROM repos $cond;
XxX
  $self->stmtExplain($sthexpl, @cval) if $ENV{'DBI_PROFILE'};
  my $rows = $sth->execute(@cval) or croak("Could not execute >".$sth->{Statement}."<: ".$sth->errstr);
  $rows = 0 if $rows eq "0E0";

  if ( $rows or $options{'force'} ) {
#      if ( $options{'force'} ) {
#          print "[ANALYZE ..." if $options{'verbose'};
#          $self->{dbh}->do("ANALYZE;");
#          print "]\n" if $options{'verbose'};
#        };

      if ( $options{'nostat'} ) {   # invalidate since they might have changed
          $self->admin('gcounti', undef);
          $self->admin('gcountu', undef);
        }
      else {
          $self->admin('gcounti', $self->idStat(undef, 'distinct' => 0) || 0);
          $self->admin('gcountu', $self->idStat(undef, 'distinct' => 1) || 0);
        }
    };

  return $rows;
}


=head3 purge ( $seqno_or_alias[, %options ] ) 

Deletes all identifiers from the database to the given pattern, 
but leaves the stored header information intact, such that it
can be updated automatically.

=over 8

=item $seqno_or_alias

  Pattern

=item Supported options: 

 force => (0|1)

Allow purging of more than one sequence.

=back


=cut

sub purge {
  my ($self, $seqno_or_alias, %options) = @_;
  $options{'verbose'} = $self->{'verbose'} unless exists $options{'verbose'};
  my @seqnos;
  if ( $seqno_or_alias && ($seqno_or_alias =~ /^\d+$/) ) {
      @seqnos = ($seqno_or_alias)}
  elsif ( $seqno_or_alias || $options{'force'} ) {
      @seqnos = $self->Seqnos('_alias', $seqno_or_alias ? ($seqno_or_alias) : ());
      unless ( @seqnos ) {
          carp("no Seqnos selected by $seqno_or_alias");
          return 0;
        };
      unless ( $options{'force'} or (@seqnos == 1) ) {
          carp("Use --force to purge more than one sequence (@seqnos)");
          return 0;
        };
    }
  else {
      carp("Use --force to purge the complete database");
      return 0;
    };
  my ($sth, $sthexpl) = $self->stmtHdl(<<"XxX");
DELETE FROM beacons WHERE seqno==?;
XxX
  my ($usth, $usthexpl) = $self->stmtHdl(<<"XxX");
UPDATE OR FAIL repos SET counti=?,countu=?,utime=?,ustat=? WHERE seqno==?;
XxX
  my $trows = 0;
  foreach my $seqno ( @seqnos ) {
      $self->stmtExplain($sthexpl, $seqno) if $ENV{'DBI_PROFILE'};
      my $rows = $sth->execute($seqno) or croak("Could not execute >".$sth->{Statement}."<: ".$sth->errstr);
      $rows = "0" if $rows eq "0E0";
      print "INFO: $rows purged for $seqno\n" if $options{'verbose'};
      $trows += $rows;
      $self->stmtExplain($usthexpl, 0, 0, time, "purged", $seqno) if $ENV{'DBI_PROFILE'};
      $usth->execute(0, 0, time, "purged", $seqno)
          or croak("Could not execute >".$usth->{Statement}."<: ".$usth->errstr);
    };

  if ( $trows or $options{'force'} ) {
#      if ( $options{'force'} ) {
#          print "[ANALYZE ..." if $options{'verbose'};
#          $self->{dbh}->do("ANALYZE;");
#          print "]\n" if $options{'verbose'};
#        };

      if ( $options{'nostat'} ) {   # invalidate since they might have changed
          $self->admin('gcounti', undef);
          $self->admin('gcountu', undef);
        }
      else {
          $self->admin('gcounti', $self->idStat(undef, 'distinct' => 0) || 0);
          $self->admin('gcountu', $self->idStat(undef, 'distinct' => 1) || 0);
        }
    };

  return $trows;
}


=head2 Methods for headers

=head3 ($rows, @oldvalues) = headerfield ( $sq_or_alias, $key [, $value] )

Gets or sets an meta or admin Entry for the constituent file indicated by $sq_or_alias

=cut

sub headerfield {
  my ($self, $sq_or_alias, $key, $value) = @_;

  my $dbkey = "";
  if ( $dbkey = SeeAlso::Source::BeaconAggregator->beaconfields($key) ) {
    }
  elsif ( $key =~ /_(\w+)$/ ) {
     $dbkey = $1}
  else {
     carp "Field $key not known";
     return undef;
    };

  my ($cond, @cval) = SeeAlso::Source::BeaconAggregator::mkConstraint($sq_or_alias);

  my ($osth, $osthexpl) = $self->stmtHdl(<<"XxX");
SELECT $dbkey FROM repos $cond;
XxX
  $self->stmtExplain($osthexpl, @cval) if $ENV{'DBI_PROFILE'};
  $osth->execute(@cval) or croak("Could not execute >".$osth->{Statement}."<:".$osth->errstr);
  my $tmpval = $osth->fetchall_arrayref();
  my @oval = map { hEncode($_, $key) } map { (defined $_->[0]) ? ($_->[0]) : () } @$tmpval;
  my $rows = scalar @oval;

  if ( (defined $value) and ($value ne "") ) {                # set
      my ($usth, $usthexpl) = $self->stmtHdl(<<"XxX");
UPDATE OR FAIL repos SET $dbkey=? $cond;
XxX
      $value = hDecode($value, $key) || "";
      $self->stmtExplain($usthexpl, $value, @cval) if $ENV{'DBI_PROFILE'};
      $rows = $usth->execute($value, @cval) or croak("Could not execute >".$usth->{Statement}."<:".$usth->errstr);
    }
  elsif ( defined $value ) {     # clear
      my ($dsth, $dsthexpl) = $self->stmtHdl(<<"XxX");
UPDATE OR FAIL repos SET $dbkey=? $cond;
XxX
      $self->stmtExplain($dsthexpl, undef, @cval) if $ENV{'DBI_PROFILE'};
      $rows = $dsth->execute(undef, @cval) or croak("Could not execute >".$dsth->{Statement}."<:".$dsth->errstr);
    }
  else {                         # read
   }

  return ($rows, @oval);
}

=head3 ($resultref, $metaref) = headers ( [ $seqno_or_alias ] ) 

Iterates over all 

For each iteration returns two hash references:

=over 8

=item 1
     all official beacon fields

=item 2
     all administrative fields (_alias, ...)

=back

=cut

sub headers {
  my ($self, $seqno_or_alias) = @_;

  unless ( $self->{_iterator_info} ) {
      my ($constraint,  @cval) = SeeAlso::Source::BeaconAggregator::mkConstraint($seqno_or_alias);
      my ($sth, $sthexpl) = $self->stmtHdl(<<"XxX");
SELECT * FROM repos $constraint;
XxX
      $self->stmtExplain($sthexpl, @cval) if $ENV{'DBI_PROFILE'};
      $sth->execute(@cval) or croak("Could not execute >".$sth->{Statement}."<: ".$sth->errstr);
      $self->{_iterator_info} = $sth;
    };

  my $info = $self->{_iterator_info}->fetchrow_hashref;
  unless ( defined $info ) {
      croak("Error listing Collections: $self->{_iterator_info}->errstr") if $self->{_iterator_info}->err;
      delete $self->{_iterator_info};
      return undef;
    }

  my $collno = $info->{seqno} || $seqno_or_alias;
  my %meta = (_seqno => $collno);
  my %result = ();
  while ( my($key, $val) = each %$info ) {
      next unless defined $val;
      my $pval = hEncode($val, $key);

      if ( $key =~ /^bc(\w+)$/ ) {
          $result{$1} = $pval}
      else {
          $meta{"_$key"} = $pval};
    }
  return \%result, \%meta;
}

=head3 listCollections ( [ $seqno_or_alias ] )

Iterates over all Sequences and returns on each call an array of

  Seqno, Alias, Uri, Modification time, Identifier Count and Unique identifier count

Returns undef if done.

=cut

sub listCollections {
  my ($self, $seqno_or_alias) = @_;

  unless ( $self->{_iterator_listCollections} ) {
      my ($constraint, @cval) = SeeAlso::Source::BeaconAggregator::mkConstraint($seqno_or_alias);
      my ($sth, $sthexpl) = $self->stmtHdl(<<"XxX");
SELECT seqno, alias, uri, mtime, counti, countu FROM repos $constraint;
XxX
      $self->stmtExplain($sthexpl, @cval) if $ENV{'DBI_PROFILE'};
      $sth->execute(@cval) or croak("Could not execute >".$sth->{Statement}."<: ".$sth->errstr);
      $self->{_iterator_listCollections} = $sth;
    };
  my $onerow = $self->{_iterator_listCollections}->fetchrow_arrayref;
  unless ( $onerow ) {
      croak("Error listing Collections: $self->{_iterator_listCollections}->errstr") if $self->{_iterator_listCollections}->err;
      delete $self->{_iterator_listCollections};
      return ();
    };
  return @$onerow;
}

=head2 Statistics

=head3 idStat ( [ $seqno_or_alias, %options ] ) 

Count identifiers for the given pattern.

=over 8

=item Supported options: 

 distinct => (0|1)

Count multiple occurences only once

 verbose => (0|1)

=back


=cut

sub idStat {
  my ($self, $seqno_or_alias, %options) = @_;
  $options{'verbose'} = $self->{'verbose'} unless exists $options{'verbose'};
  my $cond = "";
  if ( $seqno_or_alias && ($seqno_or_alias =~ /^\d+$/) ) {
      $cond = "WHERE seqno==$seqno_or_alias"}
  elsif ( $seqno_or_alias ) {
      my @seqnos = $self->Seqnos('_alias', $seqno_or_alias);
      if ( @seqnos ) {
          $cond = "WHERE seqno IN (".join(",", @seqnos).")"}
      else {
          carp("no Seqnos selected by $seqno_or_alias");
          return 0;
        };
    };
# my $count_what = $options{'distinct'} ? "DISTINCT hash" : "*";
# will not be optimized by SQLite or mySQL: SELECT COUNT($count_what) FROM beacons $cond;
# my $sth= $self->stmtHdl("SELECT COUNT($count_what) FROM beacons $cond LIMIT 1;");
  my $from = $options{'distinct'} ? "(SELECT DISTINCT hash FROM beacons $cond)"
                                  : "beacons $cond";
  my ($sth, $sthexpl) = $self->stmtHdl(<<"XxX");
SELECT COUNT(*) FROM $from LIMIT 1;
XxX
  $self->stmtExplain($sthexpl) if $ENV{'DBI_PROFILE'};
  $sth->execute() or croak("Could not execute >".$sth->{Statement}."<: ".$sth->errstr);
  my $hits = $sth->fetchrow_arrayref;

  return $hits->[0] || 0;
};


=head3 idCounts ( [ $pattern, %options ] ) 

Iterates through the entries according to the optional id filter expression.

For each iteration the call returns a triple consisting of (identifier,
number of rows, and sum of all individual counts). 

=over 8

=item Supported options: 

 distinct => (0|1)

Count multiple occurences in one beacon file only once.

=back

=cut

sub idCounts {
  my ($self, $pattern, %options) = @_;
  my $cond = $pattern ? qq!WHERE hash LIKE "$pattern"! : "";
  my $count_what = $options{'distinct'} ? "DISTINCT seqno" : "seqno";
  unless ( $self->{_iterator_idCounts} ) {
      my ($sth, $sthexpl) = $self->stmtHdl(<<"XxX");
SELECT hash, COUNT($count_what), SUM(hits) FROM beacons $cond GROUP BY hash ORDER BY hash;
XxX
      $self->stmtExplain($sthexpl) if $ENV{'DBI_PROFILE'};
      $sth->execute() or croak("Could not execute >".$sth->{Statement}."<: ".$sth->errstr);
      $self->{_iterator_idCounts} = $sth;
      unless ( defined $self->{identifierClass} ) {
          my $package = $self->autoIdentifier();
          $options{'verbose'} && ref($package) && carp "Assuming identifiers of type ".ref($package)."\n";
        }
    };
  my $onerow = $self->{_iterator_idCounts}->fetchrow_arrayref;
  unless ( $onerow ) {
      croak("Error listing Collections: $self->{_iterator_idCounts}->errstr") if $self->{_iterator_idCounts}->err;
      delete $self->{_iterator_idCounts};
      return ();
    };
  if ( defined $self->{identifierClass} ) {
      my $c = $self->{identifierClass};
# compat: hash might not take an argument, must resort to value, has to be cleared before...
      $c->value("");
      my $did = $c->hash($onerow->[0]) || $c->value($onerow->[0]);
      $onerow->[0] = $c->can("pretty") ? $c->pretty() : $c->value();
    };
  return @$onerow;
};


=head3 idList ( [ $pattern ] ) 

Iterates through the entries according to the optional selection.

For each iteration the call returns a tuple consisting of identifier and an 
list of array references (Seqno, Hits, Info, explicit Link, AltId) or the emtpy list
if finished. 

Hits, Info, Link and AltId are normalized to the empty string if undefined (or < 2 for hits).

It is important to finish all iterations before calling this method for "new" arguments:

 1 while $db->idList();  # flush pending results

=cut

sub idList {
  my ($self, $pattern) = @_;
  my $cond = $pattern ? ($pattern =~ /%/ ? "WHERE hash LIKE ?" : qq"WHERE hash=?")
                      : "";
  unless ( $self->{_iterator_idList_handle} ) {
      my ($sth, $sthexpl) = $self->stmtHdl(<<"XxX");
SELECT hash, seqno, hits, info, link, altid FROM beacons $cond ORDER BY hash, seqno, altid;
XxX
      $self->stmtExplain($sthexpl, ($pattern ? ($pattern) : () )) if $ENV{'DBI_PROFILE'};
      $sth->execute(($pattern ? ($pattern) : () )) or croak("Could not execute >".$sth->{Statement}."<: ".$sth->errstr);
      $self->{_iterator_idList_handle} = $sth;
      $self->{_iterator_idList_crosscheck} = $self->RepoCols("ALTTARGET");
      $self->{_iterator_idList_prefetch} = undef;
      $self->autoIdentifier() unless defined $self->{identifierClass};
    };
  unless ( exists $self->{_iterator_idList_prefetch} ) {   # deferred exit
      delete $self->{_iterator_idList_handle};
      delete $self->{_iterator_idList_crosscheck};
      return ();
    };
  my $pf = $self->{_iterator_idList_prefetch};
  while ( my $onerow = $self->{_iterator_idList_handle}->fetchrow_arrayref ) {
#      $onerow->[2] = "" unless $self->{_iterator_idList_crosscheck}->{$onerow->[1]};  # kill artefacts
      $onerow->[2] = "" unless $onerow->[2];  # kill artefacts
      $onerow->[3] = "" unless defined $onerow->[3];  # kill artefacts
      $onerow->[4] = "" unless defined $onerow->[4];  # kill artefacts
      $onerow->[5] = "" unless defined $onerow->[5];  # kill artefacts
      if ( defined $self->{identifierClass} ) {
          my $c = $self->{identifierClass};
# compat: hash might not take an argument, must resort to value, has to be cleared before...
          $c->value("");
          my $did = $c->hash($onerow->[0]) || $c->value($onerow->[0]);
          $onerow->[0] = $c->can("pretty") ? $c->pretty() : $c->value();
        };
      if ( $pf ) {
          if ( $pf->[0] eq $onerow->[0] ) {
              push(@$pf, [@$onerow[1..@$onerow-1]]);
              next;
            }
          else {
              $self->{_iterator_idList_prefetch} = [$onerow->[0], [@$onerow[1..@$onerow-1]]];
              return @$pf;
            }
         }
       else {
           $pf = [$onerow->[0], [@$onerow[1..@$onerow-1]]]};
    };
  
  if ( $self->{_iterator_idList_handle}->err ) {
      croak("Error listing Collections: $self->{_iterator_idList_handle}->errstr");
    };
  delete $self->{_iterator_idList_prefetch};
  return $pf ? @$pf : ();
};


=head2 Manipulation of global metadata: Open Search Description

=head3 setOSD ( $field, @values }

Sets the field $field of the OpenSearchDescription to @value(s).

=cut

sub setOSD {
  my ($self) = shift;
  $self->clearOSD($_[0]) or return undef;
  return (defined $_[1]) ? $self->addOSD(@_) : 0;     # value(s) to set
};

=head3 clearOSD ( $field }

Clears the field $field of the OpenSearchDescription.

=cut

sub clearOSD {
  my ($self, $field) = @_;
  $field || (carp("no OSD field name provided"), return undef);
  defined $self->osdKeys($field) || (carp("no valid OSD field '$field'"), return undef);
  my ($sth, $sthexpl) = $self->stmtHdl(<<"XxX");
DELETE FROM osd WHERE key=?;
XxX
  $self->stmtExplain($sthexpl, $field) if $ENV{'DBI_PROFILE'};
  $sth->execute($field) or croak("Could not execute >".$sth->{Statement}."<: ".$sth->errstr);
  return 1;
}

=head3 addOSD ( $field, @values }

Adds more @value(s) as (repeatable) field $field of the OpenSearchDescription.

=cut

sub addOSD {
  my ($self, $field, @values) = @_;
  $field || (carp("no OSD field name provided"), return undef);
  return 0 unless @values;
  defined $self->osdKeys($field) || (carp("no valid OSD field '$field'"), return undef);
  my ($sth, $sthexpl) = $self->stmtHdl(<<"XxX");
INSERT INTO osd ( key, val ) VALUES ( ?, ? );
XxX
  $self->stmtExplain($sthexpl, $field, $values[0]) if $ENV{'DBI_PROFILE'};
  my $tstatus = [];
  my $tuples = $sth->execute_array({ArrayTupleStatus => $tstatus}, $field, \@values) or croak("Could not execute >".$sth->{Statement}."<: ".$sth->errstr);
  return $tuples;
}

=head2 Manipulation of global metadata: Beacon Metadata

These headers are used when you will be publishing a beacon file for the collection.

=head3 setBeaconMeta ( $field, $value )

Sets the field $field of the Beacon meta table (used to generate a BEACON file for this
service) to $value.

=cut

sub setBeaconMeta {
  my ($self) = shift;
  $self->clearBeaconMeta(@_) or return undef;
  return (defined $_[1]) ? $self->addBeaconMeta(@_) : 0;     # value to set
};

=head3 clearBeaconMeta ( $field }

Deletes the field $field of the Beacon meta table.

=cut

sub clearBeaconMeta {
  my ($self, $rfield) = @_;
  $rfield || (carp("no Beacon field name provided"), return undef);
  my $field = $self->beaconfields($rfield) or (carp("no valid Beacon field '$rfield'"), return undef);
  my ($sth, $sthexpl) = $self->stmtHdl(<<"XxX");
DELETE FROM osd WHERE key=?;
XxX
  $self->stmtExplain($sthexpl, $field) if $ENV{'DBI_PROFILE'};
  $sth->execute($field) or croak("Could not execute >".$sth->{Statement}."<: ".$sth->errstr);
  return 1;
}

=head3 addBeaconMeta ( $field, $value )

Appends $value to the field $field of the BEACON meta table

=cut
sub addBeaconMeta {
  my ($self, $rfield, $value) = @_;
  $rfield || (carp("no Beacon field name provided"), return undef);
  my $field = $self->beaconfields($rfield) or (carp("no valid Beacon field '$rfield'"), return undef);
  my ($sth, $sthexpl) = $self->stmtHdl(<<"XxX");
INSERT INTO osd ( key, val ) VALUES ( ?, ? );
XxX
  $self->stmtExplain($sthexpl, $field, $value) if $ENV{'DBI_PROFILE'};
  $sth->execute($field, $value) or croak("Could not execute >".$sth->{Statement}."<: ".$sth->errstr);
  return 1;
}

=head3 admin ( [$field, [$value]] )

Manipulates the admin table.

Yields a hashref to the admin table if called without arguments.

If called with $field, returns the current value, and sets the
table entry to $value if defined.


=cut 

sub admin {
  my ($self, $field, $value) = @_;
  my $admref =  $self->admhash();
  return $admref unless $field;
  my $retval = $admref->{$field};
  return $retval unless defined $value;

  my ($admh, $admexpl) = $self->stmtHdl("INSERT OR REPLACE INTO admin VALUES (?, ?);");
  $self->stmtExplain($admexpl, $field, $value) if $ENV{'DBI_PROFILE'};
  $admh->execute($field, $value)
       or croak("Could not execute update to admin table: ".$admh->errstr);
  return defined($retval) ? $retval : "";
}


# on-the-fly conversions

sub hDecode {      # external time to numeric timestamp, printf placeholders
  my ($val, $fnam) = @_;
  return $val unless $fnam;
  local($_) = (ref $val) ? $val->{$fnam} : $val;
  return undef unless defined $_;

  if    ( $fnam =~ /target$/i )  { s/%/%%/g; s/(\{id\}|\$PND)/%1\$s/gi; s/(\{altid\}|\$PND)/%2\$s/gi; }
  elsif ( $fnam =~ /message$/i ) { s/%/%%/g; s/\{hits?\}/%s/gi;     }
  elsif ( $fnam =~ /time|revisit/i ) {
      if ( /^\d+$/ ) {     # legacy UNIX timestamp
        }
      elsif ( my $p = HTTP::Date::str2time($_, "GMT") ) {  # all unqualified times are GMT
          $_ = $p}
      else {
          carp("could not parse value '$_' as time in field $fnam");
          return undef;
        };
    }
  return $_;
}

sub hEncode {     # timestamp to beacon format
  my ($val, $fnam) = @_;
  local($_) = (ref $val) ? $val->{$fnam} : $val;
  return undef unless defined $_;
  if    ( $fnam =~ /time|revisit/i ) { $_ = SeeAlso::Source::BeaconAggregator::tToISO($_) }
  elsif ( $fnam =~ /message/i )      { s/%s/{hits}/; s/%%/%/g; }
  elsif ( $fnam =~ /target/i )       { s/%s/{ID}/;   s/%1\$s/{ID}/;   s/%2\$s/{ALTID}/;   s/%%/%/g; };
  return $_;
}

=head1 AUTHOR

    Thomas Berger
    CPAN ID: THB
    gymel.com
    THB@cpan.org

=head1 COPYRIGHT

This program is free software; you can redistribute
it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the
LICENSE file included with this module.

=cut

1;

