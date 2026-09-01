# Builds the zmqcat command line from module options, shared by NixOS and
# nix-darwin so the two cannot drift.
{ lib }:

{
  # join accepts only --listen, --forward, and --quiet. Passing a serve-only
  # flag such as --heartbeat makes it exit 2 before the bus comes up.
  mkArgs = cfg:
    let
      serveArgs =
        lib.optionals (cfg.heartbeat != "") [ "--heartbeat" cfg.heartbeat ]
        ++ lib.optionals cfg.local [ "--local" ]
        ++ lib.optionals (cfg.mailbox != null) [ "--mailbox" cfg.mailbox ]
        ++ lib.concatMap (k: [ "--allow" k ]) cfg.allow;
    in
    [ cfg.role "--listen" cfg.listen ]
    ++ lib.optionals (cfg.role == "serve") serveArgs
    ++ cfg.extraArgs;

  assertions = cfg: [
    {
      assertion = cfg.role != "join" || cfg.tokenFile != null;
      message = "services.zmqcat.tokenFile is required when role = \"join\".";
    }
    {
      assertion = cfg.role != "serve" || cfg.tokenFile == null;
      message = "services.zmqcat.tokenFile only applies to role = \"join\".";
    }
    {
      assertion = cfg.role != "join" || cfg.allow == [ ];
      message = "services.zmqcat.allow only applies to role = \"serve\".";
    }
  ];
}
