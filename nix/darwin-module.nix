self:
{ config, lib, pkgs, ... }:

let
  cfg = config.services.zmqcat;
  zlib = import ./lib.nix { inherit lib; };
  args = zlib.mkArgs cfg;
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
    logFile = lib.mkOption {
      type = lib.types.str;
      default = "/var/log/zmqcat.log";
      description = "Where launchd writes stdout and stderr.";
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = zlib.assertions cfg;

    launchd.daemons.zmqcat = {
      script = "exec ${startScript}";
      serviceConfig = {
        Label = "org.nixos.zmqcat";
        RunAtLoad = true;
        KeepAlive = true;
        StandardOutPath = cfg.logFile;
        StandardErrorPath = cfg.logFile;
      };
    };
  };
}
