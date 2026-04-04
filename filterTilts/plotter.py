from pathlib import Path
import matplotlib.pyplot as plt

class FilterPlotter:
    """Generate plots for filtered tilt series."""
    
    def __init__(self, output_folder, threads=24):
        """
        Initialize plotter.
        
        Parameters:
        - output_folder: Directory for output plots
        - threads: Number of parallel workers for image loading
        """
        self.output_folder = Path(output_folder)
        self.threads = threads
        
    def plot_tilt_statistics(self, ts, plot=None):
        """
        Plot basic tilt statistics (CTF vs tilt angle).
        
        Parameters:
        - ts: tiltSeriesMeta object
        - plot: Plotting option (if None or False, skip plotting)
        """
        if not plot:
            return
            
        df = ts.all_tilts_df
        
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(
            df['rlnTomoNominalStageTiltAngle'], 
            df['rlnCtfMaxResolution'],
            alpha=0.5
        )
        ax.set_xlabel('Tilt Angle (degrees)')
        ax.set_ylabel('CTF Max Resolution (Å)')
        ax.set_title('Tilt Series Statistics')
        ax.grid(True, alpha=0.3)
        
        output_path = self.output_folder / 'tiltseriesStatistic.pdf'
        plt.savefig(output_path, bbox_inches='tight')
        plt.close()
        
        print(f"Statistics plot saved: {output_path}")
        
    def plot_filter_results(self, ts, class_label_name='cryoBoostDlLabel', 
                           pred_score_label_name='cryoBoostDlProbability',
                           tilt_name_label=None, plot=False):
        """
        Plot detailed filter results with images.
        
        Parameters:
        - ts: tiltSeriesMeta object
        - class_label_name: Column name for classification labels
        - pred_score_label_name: Column name for prediction probabilities
        - tilt_name_label: Column name for tilt identifiers
        - plot: Plotting option
        """
        if not plot:
            return
            
        # Import here to avoid loading heavy dependencies if not plotting
        from src.prediction.image_processor import ImageProcessor
        
        print(f"\nGenerating filter results plot: {self.output_folder}/logfile.pdf")
        
        df = ts.all_tilts_df
        pred_labels = df[class_label_name]
        pred_probs = df[pred_score_label_name] if pred_score_label_name in df.columns else None
        pred_names = df[tilt_name_label] if tilt_name_label and tilt_name_label in df.columns else None
        
        tilts_path = ts.getMicrographMovieNameFull()
        
        # Limit to 100 images max for reasonable plot size
        max_images = min(100, len(pred_labels))
        num_cols = 4
        num_rows = (max_images + num_cols - 1) // num_cols
        
        print(f"Plotting {max_images} images ({num_rows} rows × {num_cols} cols)")
        
        # Load images
        image_processor = ImageProcessor(sz=128, threads=self.threads)
        pil_images = image_processor.load_images_parallel(tilts_path[:max_images])
        
        # Create figure
        fig, axs = plt.subplots(num_rows, num_cols, figsize=(20, 5 * num_rows))
        axs = axs.flatten() if num_rows > 1 else [axs] if num_rows == 1 else []
        
        for i, ax in enumerate(axs):
            if i < max_images:
                self._plot_single_image(
                    ax, i, pil_images, tilts_path, pred_labels, 
                    pred_probs, pred_names
                )
            else:
                ax.axis('off')
        
        plt.tight_layout()
        output_path = self.output_folder / 'logfile.pdf'
        fig.savefig(output_path)
        plt.close()
        
        print(f"Filter results plot saved: {output_path}\n")
        
    def _plot_single_image(self, ax, ind, pil_images, tilts_path, pred_labels, 
                          pred_probs, pred_names):
        """Plot a single image with prediction results."""
        img = pil_images[ind]
        ax.imshow(img, cmap='gray')
        
        # Build title
        if pred_names is not None and pred_probs is not None:
            title = f'{pred_names.iloc[ind]}\nPred: {pred_labels.iloc[ind]}, Prob: {pred_probs.iloc[ind]:.2f}'
        elif pred_probs is not None:
            title = f'Pred: {pred_labels.iloc[ind]}, Prob: {pred_probs.iloc[ind]:.2f}'
        else:
            title = f'Pred: {pred_labels.iloc[ind]}'
            
        ax.set_title(title, fontsize=9)
        
        # Add colored border
        img_size = img.size  # PIL image
        rect_width = img_size[0] * 0.98
        rect_height = img_size[1] * 0.98
        rect_x = (img_size[0] - rect_width) / 2
        rect_y = (img_size[1] - rect_height) / 2
        
        color = 'g' if pred_labels.iloc[ind] == 'good' else 'r'
        rect = plt.Rectangle(
            (rect_x, rect_y), rect_width, rect_height,
            linewidth=5, edgecolor=color, facecolor='none'
        )
        ax.add_patch(rect)
        ax.axis('off')



