from math import sqrt

def cosine_distance(v1, v2):
    # Ensure vectors are of the same length
    if len(v1) != len(v2):
        raise ValueError("Vectors must be of the same length")
    dot_product = sum(a * b for a, b in zip(v1, v2))
    magnitude1 = sqrt(sum(a * a for a in v1))
    magnitude2 = sqrt(sum(b * b for b in v2))
    if magnitude1 == 0 or magnitude2 == 0:
        return 0.0  # or handle zero magnitude appropriately
    return dot_product / (magnitude1 * magnitude2)
