import pandas as pd
from collections import Counter
import math
import warnings
from scipy import stats
import numpy as np

from sklearn.metrics import confusion_matrix

class MetricUtils:
    """
    References:
    [1] https://github.com/shakedzy/dython/blob/master/dython/nominal.py
    """
    def __init__(self):
        pass

    def conditional_entropy(self, x, y):
        y_counter = Counter(y)
        xy_counter = Counter(list(zip(x, y)))
        total_occurences = sum(y_counter.values())
        entropy = 0.0
        for xy in xy_counter.keys():
            p_xy = xy_counter[xy] / total_occurences
            p_y = y_counter[xy[1]] / total_occurences
            entropy += p_xy * math.log(p_y / p_xy, math.e)
        
        return entropy

    def cramers_v(self, x, y, bias_correction = True):
        confusion_matrix = pd.crosstab(x, y)
        chi2 = stats.chi2_contingency(confusion_matrix)[0]
        n = confusion_matrix.sum().sum()
        phi2 = chi2 / n
        r, k = confusion_matrix.shape
        if bias_correction:
            phi2corr = max(0, phi2 - ((k - 1) * (r - 1)) / (n -1))
            rcorr = r - ((r - 1) ** 2) / (n - 1)
            kcorr = k - ((k - 1) ** 2) / (n - 1)
            if min((kcorr - 1), (rcorr - 1)) == 0:
                warnings.warn(
                    "Unable to calculate Cramer's V using bias correction. Consider using bias_correction=False",
                    RuntimeWarning)
                return np.nan
            else:
                return np.sqrt(phi2corr / min((kcorr - 1), (rcorr - 1)))
        else:
            return np.sqrt(phi2 / min(k - 1, r - 1))

    def theils_u(self, x, y):
        s_xy = self.conditional_entropy(x, y)
        x_counter = Counter(x)
        total_occurrences = sum(x_counter.values())
        p_x = list(map(lambda n: n / total_occurrences, x_counter.values()))
        s_x = stats.entropy(p_x)
        if s_x == 0:
            return 1.
        else:
            s_diff = s_x - s_xy
            if -1e-13 <= s_diff < 0.:
                warnings.warn(f'Rounded U = {s_diff} to zero. This is probably due to computation errors as a result of almost insignificant differences between x and y.', RuntimeWarning)
                return 0.
            else:
                return (s_diff) / s_x

    def correlation_ratio(self, categories, measurements):
        """
        categories: pass it as values eg: data.values
        measurements: pass it as values eg: data.values
        """
        fcat, _ = pd.factorize(categories)
        cat_num = np.max(fcat) + 1
        y_avg_array = np.zeros(cat_num)
        n_array = np.zeros(cat_num)
        for i in range(0, cat_num):
            cat_measures = measurements[np.argwhere(fcat == i).flatten()]
            n_array[i] = len(cat_measures)
            y_avg_array[i] = np.average(cat_measures)
        y_total_avg = np.sum(np.multiply(y_avg_array, n_array)) / np.sum(n_array)
        numerator = np.sum(
            np.multiply(n_array, np.power(np.subtract(y_avg_array, y_total_avg),
                                      2)))
        denominator = np.sum(np.power(np.subtract(measurements, y_total_avg), 2))
        if numerator == 0:
            eta = 0.0
        else:
            eta = np.sqrt(numerator / denominator)
        return eta