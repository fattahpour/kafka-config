# Kafka Config: spatial UI review

## Existing strengths

The five-step workflow, plain-language presets, state normalization, broker failure simulation,
lag fixes, configuration import/share and multi-language export already form a useful product.
The SVG map exposes precise offsets and replica states, so it remains the detailed view.

## Problems addressed

1. The large SVG requires horizontal scrolling on phones. The optional 3D camera fits the
   topology to the viewport, with explicit rotation/zoom controls and the original 2D fallback.
2. Visual topology and configuration are separated. Component inspectors now explain the
   selected object and link directly to its configuration step.
3. Failure consequences need more immediate context. Online brokers, writable partitions,
   replica copies and total group lag appear beside the scene; partition inspectors show
   the leader, replicas, high watermark and consumer assignments.
4. Adding 3D could make first load and animation expensive. The renderer loads lazily,
   uses shared low-poly geometry and instanced particles, caps resolution/fps and stops
   rendering when paused, offscreen or hidden. Hidden SVG animation work is skipped.
5. Canvas-only interfaces are difficult to use by keyboard. DOM component buttons,
   partition buttons, explicit camera controls and the full SVG view remain available.

## Implementation boundaries

The renderer reads existing simulation state. Configuration defaults, Kafka behavior,
imports, saved setups, challenges and generated client/server files are retained.
The camera and quality selection do not modify exported settings. Broker failure remains
an educational simulation, never a connection to or operation on a real Kafka cluster.

The original single-page packaging remains intact. Three.js 0.160.1 is pinned with SRI
and loaded from jsDelivr, with an identical npm development dependency for tests.
The standalone HTML retains a network dependency for its optional 3D view.

## Validation

- Existing browser regression suite retained.
- New browser tests cover 3D state consistency, broker failure/election, export invariance,
  camera controls, Eco resolution, dense mobile topology, reduced motion, context loss,
  and dependency failure.
- CI uploads desktop/mobile screenshots for visual review.
- Build and JavaScript parse checks can run without a browser.
- Physical-device performance and Safari/Firefox behavior require separate verification;
  a frame cap is a resource budget, not a measured performance guarantee.
