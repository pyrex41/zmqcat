{
  description = "zmqcat — mailbox bus over Tailcat";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [ "aarch64-darwin" "x86_64-darwin" "aarch64-linux" "x86_64-linux" ];
      forAll = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      overlays.default = final: prev: {
        zmqcat = final.callPackage ./nix/package.nix { };
      };

      packages = forAll (pkgs: rec {
        zmqcat = pkgs.callPackage ./nix/package.nix { };
        default = zmqcat;
      });

      devShells = forAll (pkgs: {
        default = pkgs.mkShell {
          packages = [ pkgs.go pkgs.gopls ];
        };
      });

      nixosModules.default = import ./nix/nixos-module.nix self;
      darwinModules.default = import ./nix/darwin-module.nix self;

      formatter = forAll (pkgs: pkgs.nixpkgs-fmt);
    };
}
