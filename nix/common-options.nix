# Options shared by the NixOS and nix-darwin zmqcat modules.
{ lib }:

with lib;
{
  enable = mkEnableOption "the zmqcat bus";

  package = mkOption {
    type = types.package;
    description = "zmqcat package to run.";
  };

  role = mkOption {
    type = types.enum [ "serve" "join" ];
    default = "serve";
    description = ''
      serve owns the bus and prints a Tailcat token other hosts join with.
      join dials an existing bus and exposes it as a local socket.
    '';
  };

  listen = mkOption {
    type = types.str;
    default = "unix:///tmp/zmqcat.sock";
    description = "Local sidecar address every app on this host talks to.";
  };

  local = mkOption {
    type = types.bool;
    default = false;
    description = "serve only: same-host bus, no Tailcat overlay.";
  };

  mailbox = mkOption {
    type = types.nullOr types.str;
    default = null;
    example = "/var/lib/zmqcat/mailbox.json";
    description = ''
      Durable mailbox state file. Null keeps queues in memory, so jobs are
      lost on restart.
    '';
  };

  allow = mkOption {
    type = types.listOf types.str;
    default = [ ];
    example = [ "nodekey:abc123…" ];
    description = ''
      serve only: restrict which Tailcat clients may dial in. Empty means
      anyone holding the token can reach the bus, and the bus has no
      mailbox-level ACLs of its own.
    '';
  };

  tokenFile = mkOption {
    type = types.nullOr types.path;
    default = null;
    description = ''
      join only: file containing the tc… token printed by the serving host.
      A path, never a literal — anything inline lands in the world-readable
      Nix store.
    '';
  };

  heartbeat = mkOption {
    type = types.str;
    default = "5s";
    description = "Session liveness interval.";
  };

  extraArgs = mkOption {
    type = types.listOf types.str;
    default = [ ];
    description = "Additional zmqcat arguments.";
  };
}
