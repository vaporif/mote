{
  description = "Rust project";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    devshells.url = "github:vaporif/nix-devshells";
    devshells.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = {
    nixpkgs,
    devshells,
    ...
  }: let
    systems = ["x86_64-linux" "aarch64-darwin"];
    forAllSystems = nixpkgs.lib.genAttrs systems;
  in {
    formatter = forAllSystems (system: nixpkgs.legacyPackages.${system}.alejandra);

    devShells = forAllSystems (system: {
      default = devshells.devShells.${system}.rust;
    });
  };
}
