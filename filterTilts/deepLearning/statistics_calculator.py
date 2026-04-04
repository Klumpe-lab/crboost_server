import numpy as np


class PredictionThresholder:
    """
    Handles probability thresholding for predictions.
    
    Used during real-time prediction to adjust low-confidence predictions.
    """
    
    def __init__(self, prob_threshold=0.1, prob_action="assignToGood"):
        """
        Initialize thresholder.
        
        Parameters:
        - prob_threshold: Probability threshold for uncertain predictions
        - prob_action: Action for low-probability predictions 
                      ('assignToGood', 'assignToBad')
        """
        self.prob_threshold = prob_threshold
        self.prob_action = prob_action
        
    def apply_threshold(self, pred_labels, pred_probs):
        """
        Apply probability threshold to predictions.
        
        Parameters:
        - pred_labels: List of predicted labels
        - pred_probs: List of prediction probabilities
        
        Returns:
        - Tuple of (adjusted_labels, adjusted_probs)
        """
        adjusted_labels = pred_labels.copy()
        adjusted_probs = pred_probs.copy()
        
        adjusted_count = 0
        for i, prob in enumerate(pred_probs):
            if prob < self.prob_threshold:
                adjusted_count += 1
                if self.prob_action == "assignToGood":
                    adjusted_labels[i] = "good"
                elif self.prob_action == "assignToBad":
                    adjusted_labels[i] = "bad"
        
        if adjusted_count > 0:
            print(f"Adjusted {adjusted_count} predictions below threshold {self.prob_threshold}")
                    
        return adjusted_labels, adjusted_probs
    
    def get_threshold_stats(self, pred_probs):
        """
        Get statistics about threshold application.
        
        Parameters:
        - pred_probs: List of prediction probabilities
        
        Returns:
        - Dictionary with threshold statistics
        """
        below_threshold = sum(1 for p in pred_probs if p < self.prob_threshold)
        
        return {
            'total_predictions': len(pred_probs),
            'below_threshold': below_threshold,
            'below_threshold_percentage': (below_threshold / len(pred_probs) * 100) if pred_probs else 0,
            'threshold_value': self.prob_threshold,
            'action': self.prob_action
        }


class FilterStatistics:
    """
    Calculates comprehensive statistics for filtered tilt series.
    
    Analyzes prediction results with domain-specific knowledge
    (tilt angles, out-of-distribution detection).
    """
    
    def __init__(self, ts, len_unfiltered, prob_threshold=0.1):
        """
        Initialize and calculate statistics.
        
        Parameters:
        - ts: tiltSeriesMeta object with filtering labels and probabilities
        - len_unfiltered: Original number of tilts before filtering
        - prob_threshold: Probability threshold for reporting (default: 0.1)
        """
        self.prob_threshold = prob_threshold
        self.len_unfiltered = len_unfiltered
        
        # Statistics attributes (calculated immediately)
        self.mean_prob = None
        self.mean_ang_bad = None
        self.mean_ang_good = None
        self.bad_fraction = None
        self.is_out_of_distribution = False
        self.stats = {}
        
        # Calculate everything immediately
        self._calculate(ts)
        
    def _calculate(self, ts):
        """
        Internal method to calculate statistics.
        
        Parameters:
        - ts: tiltSeriesMeta object with filtering results
        """
        df = ts.all_tilts_df
        
        # Basic counts
        total_after = len(df)
        bad_count = (df['cryoBoostDlLabel'] == "bad").sum()
        good_count = (df['cryoBoostDlLabel'] == "good").sum()
        
        # Mean probability
        self.mean_prob = df['cryoBoostDlProbability'].mean()
        pred_probs = df['cryoBoostDlProbability'].tolist()
        
        # Tilt angles
        self._calculate_angle_statistics(df)
        
        # Bad fraction
        self.bad_fraction = round((bad_count / self.len_unfiltered) * 100, 1)
        
        # Low confidence count
        low_confidence_count = sum(1 for p in pred_probs if p < self.prob_threshold)
        
        # Store in dict
        self.stats = {
            'total_before': self.len_unfiltered,
            'total_after': total_after,
            'good_count': good_count,
            'bad_count': bad_count,
            'good_percentage': (good_count / total_after * 100) if total_after > 0 else 0,
            'bad_percentage': (bad_count / total_after * 100) if total_after > 0 else 0,
            'mean_probability': self.mean_prob,
            'std_probability': np.std(pred_probs),
            'min_probability': np.min(pred_probs),
            'max_probability': np.max(pred_probs),
            'low_confidence_count': low_confidence_count,
            'low_confidence_percentage': (low_confidence_count / total_after * 100) if total_after > 0 else 0,
            'mean_angle_good': self.mean_ang_good,
            'mean_angle_bad': self.mean_ang_bad,
            'bad_fraction': self.bad_fraction,
            'tomogram_count': ts.nrTomo
        }
        
        return self.stats
    
    def _calculate_angle_statistics(self, df):
        """Calculate mean tilt angles for good and bad tilts."""
        bad_df = df[df['cryoBoostDlLabel'] == "bad"]
        good_df = df[df['cryoBoostDlLabel'] == "good"]
        
        self.mean_ang_bad = bad_df['rlnTomoNominalStageTiltAngle'].abs().mean()
        self.mean_ang_good = good_df['rlnTomoNominalStageTiltAngle'].abs().mean()
        
        # Handle case where no bad tilts exist
        if np.isnan(self.mean_ang_bad):
            self.mean_ang_bad = float(180)
    
    def evaluate_distribution(self):
        """
        Determine if data is out of distribution.
        
        Checks for:
        - Low mean probability (< 0.95)
        - Good tilts at higher angles than bad tilts
        - Excessive bad tilt fraction (> 25%)
        """
        self.is_out_of_distribution = (
            self.mean_prob < 0.95 or 
            self.mean_ang_good > (self.mean_ang_bad - 2) or 
            self.bad_fraction > 25
        )
        
        self.stats['is_out_of_distribution'] = self.is_out_of_distribution

        return self.is_out_of_distribution

    
    def print_summary(self):
        """Print comprehensive statistics summary."""
        if not self.stats:
            print("No statistics calculated yet. Call calculate() first.")
            return
        
        print(f"\n{'='*70}")
        print("Filter Statistics")
        print(f"{'='*70}")
        print(f"  Total tilts (before): {self.stats['total_before']}")
        print(f"  Total tilts (after): {self.stats['total_after']}")
        print(f"  Good tilts: {self.stats['good_count']} ({self.stats['good_percentage']:.1f}%)")
        print(f"  Bad tilts: {self.stats['bad_count']} ({self.stats['bad_percentage']:.1f}%)")
        print(f"\nConfidence Metrics:")
        print(f"  Mean Probability: {self.mean_prob:.4f} (should be > 0.95)")
        print(f"  Std Probability: {self.stats['std_probability']:.4f}")
        print(f"  Min/Max Probability: {self.stats['min_probability']:.4f} / {self.stats['max_probability']:.4f}")
        print(f"  Low Confidence (<{self.prob_threshold}): {self.stats['low_confidence_count']} ({self.stats['low_confidence_percentage']:.1f}%)")
        
        print(f"\nTilt Angle Analysis:")
        # Format mean angle of bad tilts
        mean_ang_bad_str = "n.d" if int(self.mean_ang_bad) == 180 else f"{self.mean_ang_bad:.1f}°"
        print(f"  Mean Angle (Good): {self.mean_ang_good:.1f}°")
        print(f"  Mean Angle (Bad): {mean_ang_bad_str}")
        print(f"  Bad Fraction: {self.bad_fraction}%")
        
        # Warning if out of distribution
        if self.is_out_of_distribution:
            print("\n⚠️  WARNING: Data appears out of distribution!")
            print("    Consider manual sorting or retraining the model.")
            
            if self.mean_prob < 0.95:
                print(f"    - Low mean probability: {self.mean_prob:.2f}")
            if self.mean_ang_good > (self.mean_ang_bad - 2):
                print(f"    - Good tilts at high angles: {self.mean_ang_good:.1f}° vs bad {self.mean_ang_bad:.1f}°")
            if self.bad_fraction > 50:
                print(f"    - High bad fraction: {self.bad_fraction}%")
        else:
            print("\n✅ Removal of bad tilts successful")
        
        if 'tomogram_count' in self.stats:
            print(f"\nTomograms processed: {self.stats['tomogram_count']}")
            
        print(f"{'='*70}\n")
    
    def get_stats_dict(self):
        """
        Get all statistics as dictionary.
        
        Returns:
        - Dictionary with all calculated statistics
        """
        return self.stats.copy()

