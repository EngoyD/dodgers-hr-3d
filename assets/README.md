# Stadium model drop-in

The viewer auto-loads `assets/dodger_stadium.glb` if present; otherwise it uses
the built-in procedural stadium.

Target model: "Dodger Stadium, Baseball Park, Los Angeles" by LibanCiel
(https://sketchfab.com/3d-models/dodger-stadium-baseball-park-los-angeles-056cff8cfd4c435187ffd7f3d31aaee4)
— paid, sold via Fab ("Get it on Fab" link on that page), Sketchfab Standard
license (allows use in derivative works, no redistribution of the asset itself).
Do not commit the purchased file to a public repo.

Steps:
1. Purchase/download from Fab with your own account.
2. If the download is glTF (.gltf + textures), convert/pack to a single .glb
   (e.g. `npx gltf-pipeline -i scene.gltf -o dodger_stadium.glb`), or ask
   Claude to convert whatever format you received (FBX/OBJ also fine).
3. Place it at `assets/dodger_stadium.glb` and reload the viewer.
4. Alignment knobs live in `CFG.model` in index.html: `scaleToFeet`, `yawDeg`,
   `offsetFt` (origin must end up at home plate, +x toward center field, feet).
   Tune live in the console via `window.__stadiumModel`, then bake values in.
