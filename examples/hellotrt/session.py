# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""One CPU ResNet50 session.

Load this after fork, once per serving loop and once in the task process.
The parent must not call :meth:`Session.load` before ``serve()``: a live
``nn.Module`` must not be copied across fork. Weights stay in the Torch
hub cache and are not stored in the repository.
"""

from __future__ import annotations

import threading
from typing import Any
from typing import List
from typing import Sequence

_CHANNELS = 3
_HEIGHT = 224
_WIDTH = 224
_IMAGE_SIZE = _CHANNELS * _HEIGHT * _WIDTH
_LOGITS = 1000
# ImageNet normalization. Inputs are NCHW floats in ``[0, 1]``.
_MEAN = (0.485, 0.456, 0.406)
_STD = (0.229, 0.224, 0.225)

# One process may start several serving loops together. Serializing the
# load keeps those threads from downloading the same checkpoint at once.
_LOAD_LOCK = threading.Lock()


class Session:
    """A ResNet50 module owned by one serving loop or one task process.

    ``eval`` mode uses the checkpoint's BatchNorm statistics, so a batched
    forward matches one forward per image.
    """

    def __init__(self, model: Any) -> None:
        self._model = model

    @classmethod
    def load(cls) -> Session:
        """Download the checkpoint if needed and return a new CPU module.

        ``torch.set_num_threads(1)`` is process-wide, so several sessions in
        one process do not each start a thread pool. The module itself is
        still new on every call.

        Returns:
            A session whose weights are already on CPU.

        Raises:
            Exception: Whatever Torch raises when the checkpoint cannot be
                downloaded or read. Startup treats that as a failed hook.
        """
        # Import here, not at module import. The parent imports this module
        # before fork and must not initialize Torch in that process.
        import torch
        from torchvision.models import ResNet50_Weights
        from torchvision.models import resnet50

        with _LOAD_LOCK:
            torch.set_num_threads(1)
            model = resnet50(weights=ResNet50_Weights.IMAGENET1K_V1)
            model.eval()
            model.to("cpu")
            return cls(model)

    def infer(self, image: Sequence[float]) -> List[float]:
        """Run one image.

        Args:
            image: ``[3, 224, 224]`` flattened in NCHW order, values in
                ``[0, 1]``.

        Returns:
            1000 logits.
        """
        return self.infer_batch([image])[0]

    def infer_batch(self, images: Sequence[Sequence[float]]) -> List[List[float]]:
        """Run ``N`` images as one ``[N, 3, 224, 224]`` forward.

        Args:
            images: Each item is one flattened ``[3, 224, 224]`` image.

        Returns:
            One length-1000 row per input, in the same order.

        Raises:
            ValueError: An image is not ``[3, 224, 224]``.
        """
        if not images:
            return []
        import torch

        batch = torch.empty((len(images), _CHANNELS, _HEIGHT, _WIDTH), dtype=torch.float32)
        for index, image in enumerate(images):
            if len(image) != _IMAGE_SIZE:
                raise ValueError(f"image length {len(image)}, want {_IMAGE_SIZE} ([3, 224, 224])")
            plane = torch.tensor(image, dtype=torch.float32).view(_CHANNELS, _HEIGHT, _WIDTH)
            for channel, (mean, std) in enumerate(zip(_MEAN, _STD)):
                plane[channel].sub_(mean).div_(std)
            batch[index].copy_(plane)
        with torch.inference_mode():
            logits = self._model(batch)
        if tuple(logits.shape) != (len(images), _LOGITS):
            raise RuntimeError(f"logits shape {tuple(logits.shape)}, want ({len(images)}, {_LOGITS})")
        return logits.tolist()
