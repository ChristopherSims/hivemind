"""Share the already-loaded, pinned local embedding model without downloads."""
import struct


class LocalSemanticEncoder:
    model_id = "eg2-text-768-v1"

    def __init__(self, encoder):
        if encoder is None:
            raise ValueError("Semantic lifecycle screening requires the loaded local encoder")
        self.encoder = encoder

    def encode(self, texts):
        # Encoder.query owns its model mutex, never a canonical database lock.
        return [list(struct.unpack("<768f", self.encoder.query(text))) for text in texts]
