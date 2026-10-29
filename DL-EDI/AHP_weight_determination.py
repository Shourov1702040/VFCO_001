import numpy as np

def ahp_weights(a12, a13, a23):

    A = np.array([
        [1.0,     a12,     a13],
        [1.0/a12, 1.0,     a23],
        [1.0/a13, 1.0/a23, 1.0]
    ], dtype=float)

    # Geometric mean method
    g = np.prod(A, axis=1) ** (1/3)
    w = g / g.sum()

    # Consistency check
    y = A @ w
    lam_max = float(np.mean(y / w))
    n = 3
    CI = float((lam_max - n) / (n - 1))
    RI = 0.58
    CR = float(CI / RI)

    return w, CR, CI, lam_max, A

# inputs priorities
a12 = 2
a13 = 4

# To match: Security > service degradation > resource consumption 
a23 = 1  # timeliness over interference = 0.3

w, CR, CI, lam_max, A = ahp_weights(a12, a13, a23)

print("Pairwise matrix A:\n", A)
print("Weights (ω1 Security, ω2 Timeliness, ω3 Interference):", w)
print("λmax:", lam_max, "CI:", CI, "CR:", CR)
