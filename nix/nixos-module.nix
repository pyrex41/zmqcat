self:
{ config, lib, pkgs, ... }:

let
  cfg = config.services.zmqcat;
  zlib = import ./lib.nix { inherit lib; };
  args = zlib.mkArgs cfg;
  # join takes the token as a positional argument, and it must come from a
  # file at runtime rather than the Nix store.
  startScript = pkgs.writeShellScript "zmqcat-start" (
    if cfg.role == "join" then ''
      exec ${lib.getExe cfg.package} join \
        ${lib.escapeShellArgs (builtins.tail args)} "$(cat ${cfg.tokenFile})"
    '' else ''
      exec ${lib.getExe cfg.package} ${lib.escapeShellArgs args}
    ''
  );
in
{
  options.services.zmqcat = import ./common-options.nix { inherit lib; } // {
    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.zmqcat;
      defaultText = lib.literalExpression "zmqcat.packages.\${system}.zmqcat";
      description = "zmqcat package to run.";
    };
    user = lib.mkOption {
      type = lib.types.str;
      default = "zmqcat";
      description = "User the bus runs as.";
    };
    group = lib.mkOption {
      type = lib.types.str;
      default = "zmqcat";
      description = "Group the bus runs as. Anything in it can reach every mailbox.";
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = zlib.assertions cfg;

    users.users = lib.mkIf (cfg.user == "zmqcat") {
      zmqcat = { isSystemUser = true; group = cfg.group; };
    };
    users.groups = lib.mkIf (cfg.group == "zmqcat") { zmqcat = { }; };

    systemd.services.zmqcat = {
      description = "zmqcat mailbox bus";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];
      serviceConfig = {
        ExecStart = startScript;
        Restart = "always";
        RestartSec = 2;
        User = cfg.user;
        Group = cfg.group;
        StateDirectory = "zmqcat";
        RuntimeDirectory = "zmqcat";
        # The socket is the whole access-control story: anything that can
        # open it can read and write every mailbox.
        UMask = "0077";
        NoNewPrivileges = true;
        PrivateTmp = lib.mkDefault false; # the default socket lives in /tmp
        ProtectSystem = "strict";
        ProtectHome = true;
        ReadWritePaths = lib.optional (cfg.mailbox != null) (builtins.dirOf cfg.mailbox);
      };
    };
  };
}
