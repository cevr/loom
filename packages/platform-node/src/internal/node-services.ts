import { NodeChildProcessSpawner, NodeFileSystem, NodePath } from "@effect/platform-node-shared";
import { Layer } from "effect";

const fileSystemAndPath = Layer.merge(NodeFileSystem.layer, NodePath.layer);

export const layerNodeServices = Layer.merge(
  fileSystemAndPath,
  NodeChildProcessSpawner.layer.pipe(Layer.provide(fileSystemAndPath)),
);
