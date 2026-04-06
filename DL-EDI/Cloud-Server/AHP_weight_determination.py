import numpy as np

def ahp_weights(a12, a13, a23):
    """
    AHP for 3 criteria:
    O1 = Security, O2 = Delay, O3 = Interference
    a12 = O1 over Objective2
    a13 = O1 over Objective3
    a23 = O2 over Objective3
    Returns: weights (w1,w2,w3), CR, plus diagnostic values.
    """
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

# # inputs priorities
# a12 = 4
# a13 = 2

# # To match: Security > service degradation > resource consumption 
# a23 = 2  # timeliness over interference = 0.3

# w, CR, CI, lam_max, A = ahp_weights(a12, a13, a23)

# print("Pairwise matrix A:\n", A)
# print("Weights (ω1 Security, ω2 Timeliness, ω3 Interference):", w)
# print("λmax:", lam_max, "CI:", CI, "CR:", CR)
