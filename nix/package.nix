{ lib, buildGoModule, go }:

buildGoModule {
  pname = "zmqcat";
  version = "0.2.0";

  src = lib.cleanSourceWith {
    src = ../.;
    filter = path: type:
      let base = baseNameOf path; in
      !(lib.hasSuffix "_test.go" base) || type == "directory";
  };

  # Dependencies are all public (tailscale, tailcat), so a normal module
  # fetch works here. huginn vendors instead, because it depends on this
  # repo while this repo is private.
  vendorHash = "sha256-ovb4Jmx60i+LxYbg52RfQayB5nay2M40nU/tW2zzIq0=";

  subPackages = [ "cmd/zmqcat" ];

  ldflags = [ "-s" "-w" ];

  meta = {
    description = "Mailbox bus over Tailcat: named mailboxes, durable jobs, pub/sub";
    mainProgram = "zmqcat";
    platforms = lib.platforms.unix;
  };
}
