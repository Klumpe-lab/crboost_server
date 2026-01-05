#!/usr/bin/env python3
"""
vis_particles.py

Standalone visualization utilities for particle lists from template matching.
Generates IMOD models, ChimeraX markers, BILD files, and CXC scripts.

Usage:
    python vis_particles.py --candidates External/job010/candidates.star \
                            --tomograms External/job010/tomograms.star \
                            --output_dir External/job010/vis \
                            --diameter 300
"""

import argparse
import subprocess
import os
import numpy as np
import pandas as pd
from pathlib import Path


def read_star_file(star_path):
    """
    Minimal star file reader. Returns dict of dataframes keyed by data block name.
    """
    blocks = {}
    current_block = None
    columns = []
    data_rows = []
    in_loop = False
    
    with open(star_path, 'r') as f:
        for line in f:
            line = line.strip()
            
            if not line or line.startswith('#'):
                continue
            
            if line.startswith('data_'):
                # save previous block if exists
                if current_block and columns and data_rows:
                    blocks[current_block] = pd.DataFrame(data_rows, columns=columns)
                current_block = line[5:]  # remove 'data_' prefix
                columns = []
                data_rows = []
                in_loop = False
                continue
            
            if line == 'loop_':
                in_loop = True
                continue
            
            if line.startswith('_'):
                # column definition
                col_name = line.split()[0][1:]  # remove leading underscore
                if '#' in col_name:
                    col_name = col_name.split('#')[0]
                columns.append(col_name)
                continue
            
            if in_loop and columns:
                # data row
                values = line.split()
                if len(values) == len(columns):
                    data_rows.append(values)
    
    # save last block
    if current_block and columns and data_rows:
        blocks[current_block] = pd.DataFrame(data_rows, columns=columns)
    
    # convert numeric columns
    for block_name, df in blocks.items():
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                pass
        blocks[block_name] = df
    
    return blocks


class ParticleVisualizer:
    """
    Generates visualization files for particle coordinates.
    """
    
    def __init__(self, candidates_star, tomograms_star):
        """
        Args:
            candidates_star: path to candidates.star (particle list)
            tomograms_star: path to tomograms.star (tomogram metadata)
        """
        self.candidates = read_star_file(candidates_star)
        self.tomograms = read_star_file(tomograms_star)
        
        # get the particle dataframe (usually 'particles' block)
        if 'particles' in self.candidates:
            self.particles_df = self.candidates['particles']
        else:
            # fallback: take the first/only block
            self.particles_df = list(self.candidates.values())[0]
        
        # get tomogram info (usually 'global' block)
        if 'global' in self.tomograms:
            self.tomo_df = self.tomograms['global']
        else:
            self.tomo_df = list(self.tomograms.values())[0]
        
        # build tomo info lookup
        self.tomo_info = {}
        for _, row in self.tomo_df.iterrows():
            tomo_name = row['rlnTomoName']
            self.tomo_info[tomo_name] = {
                'size': (int(row['rlnTomoSizeX']), 
                        int(row['rlnTomoSizeY']), 
                        int(row['rlnTomoSizeZ'])),
                'pixel_size': float(row['rlnTomoTiltSeriesPixelSize']),
                'tomogram_path': row.get('rlnTomoReconstructedTomogram', ''),
            }
    
    def get_pixel_coords(self, tomo_name):
        """
        Convert centered Angstrom coordinates to IMOD pixel coordinates.
        Returns Nx3 array of (x, y, z) in pixels.
        """
        tomo = self.tomo_info[tomo_name]
        size = np.array(tomo['size'], dtype=float)
        
        # filter particles for this tomogram
        mask = self.particles_df['rlnTomoName'] == tomo_name
        particles = self.particles_df[mask]
        
        # get pixel size from particles (template matching pixel size)
        if 'rlnTomoTiltSeriesPixelSize' in particles.columns:
            pixel_size = particles['rlnTomoTiltSeriesPixelSize'].iloc[0]
        else:
            pixel_size = tomo['pixel_size']
        
        # extract centered angstrom coords
        coords_angst = particles[['rlnCenteredCoordinateXAngst',
                                   'rlnCenteredCoordinateYAngst', 
                                   'rlnCenteredCoordinateZAngst']].values
        
        # convert: pixel = angst/pixel_size + size/2
        coords_pixel = coords_angst / pixel_size + size / 2
        
        return coords_pixel.astype(np.float32), pixel_size
    
    def get_angles(self, tomo_name):
        """Get Euler angles (rot, tilt, psi) for particles in a tomogram."""
        mask = self.particles_df['rlnTomoName'] == tomo_name
        particles = self.particles_df[mask]
        angles = particles[['rlnAngleRot', 'rlnAngleTilt', 'rlnAnglePsi']].values
        return angles
    
    def get_scores(self, tomo_name):
        """Get LCC scores for particles in a tomogram."""
        mask = self.particles_df['rlnTomoName'] == tomo_name
        particles = self.particles_df[mask]
        if 'rlnLCCmax' in particles.columns:
            return particles['rlnLCCmax'].values
        return None
    
    def write_imod_model(self, output_dir, diameter_angst, color=(0, 255, 0), 
                         thickness=2, point_mode=False):
        """
        Write IMOD model files using point2model.
        
        Args:
            output_dir: output directory
            diameter_angst: particle diameter in Angstroms (used for sphere radius)
            color: RGB tuple (0-255)
            thickness: line thickness
            point_mode: if True, use small spheres (8 pixels) instead of particle radius
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        for tomo_name in self.tomo_info.keys():
            coords, pixel_size = self.get_pixel_coords(tomo_name)
            
            if len(coords) == 0:
                continue
            
            txt_path = output_dir / f'coords_{tomo_name}.txt'
            mod_path = output_dir / f'coords_{tomo_name}.mod'
            
            # write coordinate text file
            np.savetxt(txt_path, coords, delimiter='\t', fmt='%.0f')
            
            # calculate radius in pixels
            if point_mode:
                radius_pix = 4  # small marker
            else:
                radius_pix = int(diameter_angst / (pixel_size * 2))
            
            # run point2model
            cmd = (f'point2model {txt_path} {mod_path} '
                   f'-sphere {radius_pix} -scat '
                   f'-color {color[0]},{color[1]},{color[2]} -t {thickness}')
            
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"Warning: point2model failed for {tomo_name}: {result.stderr}")
            else:
                print(f"  Written: {mod_path}")
    
    def write_chimerax_markers(self, output_dir, diameter_angst, 
                               color=(0, 255, 0, 255)):
        """
        Write ChimeraX marker files (.cmm format).
        
        Args:
            output_dir: output directory
            diameter_angst: particle diameter in Angstroms
            color: RGBA tuple (0-255)
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        for tomo_name, tomo in self.tomo_info.items():
            coords, pixel_size = self.get_pixel_coords(tomo_name)
            scores = self.get_scores(tomo_name)
            
            if len(coords) == 0:
                continue
            
            # convert to Angstroms for ChimeraX (from pixel coords)
            coords_angst = coords * pixel_size
            radius_angst = diameter_angst / 2
            
            cmm_path = output_dir / f'{tomo_name}_markers.cmm'
            
            with open(cmm_path, 'w') as f:
                f.write('<marker_set name="particles">\n')
                
                for i, (x, y, z) in enumerate(coords_angst):
                    # include score as note if available
                    note = f'score={scores[i]:.4f}' if scores is not None else ''
                    
                    f.write(f'<marker id="{i+1}" x="{x:.2f}" y="{y:.2f}" z="{z:.2f}" '
                           f'r="{color[0]/255:.3f}" g="{color[1]/255:.3f}" '
                           f'b="{color[2]/255:.3f}" radius="{radius_angst:.1f}" '
                           f'note="{note}"/>\n')
                
                f.write('</marker_set>\n')
            
            print(f"  Written: {cmm_path}")
    
    def write_bild(self, output_dir, diameter_angst, color=(0, 1, 0)):
        """
        Write BILD files (simple geometry format for ChimeraX/Chimera).
        
        Args:
            output_dir: output directory
            diameter_angst: particle diameter in Angstroms
            color: RGB tuple (0-1 float)
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        for tomo_name, tomo in self.tomo_info.items():
            coords, pixel_size = self.get_pixel_coords(tomo_name)
            
            if len(coords) == 0:
                continue
            
            coords_angst = coords * pixel_size
            radius_angst = diameter_angst / 2
            
            bild_path = output_dir / f'{tomo_name}_particles.bild'
            
            with open(bild_path, 'w') as f:
                f.write(f'.color {color[0]:.3f} {color[1]:.3f} {color[2]:.3f}\n')
                f.write(f'.transparency 0.3\n')
                
                for x, y, z in coords_angst:
                    f.write(f'.sphere {x:.2f} {y:.2f} {z:.2f} {radius_angst:.1f}\n')
            
            print(f"  Written: {bild_path}")
    
    def write_chimerax_script(self, output_dir, diameter_angst):
        """
        Write ChimeraX command scripts (.cxc) that load tomogram and markers.
        
        Args:
            output_dir: output directory  
            diameter_angst: particle diameter in Angstroms
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        for tomo_name, tomo in self.tomo_info.items():
            coords, pixel_size = self.get_pixel_coords(tomo_name)
            
            if len(coords) == 0:
                continue
            
            tomo_path = tomo.get('tomogram_path', '')
            cxc_path = output_dir / f'{tomo_name}_view.cxc'
            cmm_path = output_dir / f'{tomo_name}_markers.cmm'
            bild_path = output_dir / f'{tomo_name}_particles.bild'
            
            with open(cxc_path, 'w') as f:
                f.write(f'# ChimeraX script for {tomo_name}\n')
                f.write(f'# Generated by vis_particles.py\n\n')
                
                # load tomogram if path exists
                if tomo_path and os.path.exists(tomo_path):
                    f.write(f'# Load tomogram\n')
                    f.write(f'open "{tomo_path}"\n')
                    f.write(f'volume #1 voxelSize {pixel_size},{pixel_size},{pixel_size}\n')
                    f.write(f'volume #1 style surface level 0.02\n\n')
                else:
                    f.write(f'# Tomogram path: {tomo_path}\n')
                    f.write(f'# (update path if needed)\n\n')
                
                # load markers
                f.write(f'# Load particle markers\n')
                if cmm_path.exists():
                    f.write(f'open "{cmm_path}"\n')
                f.write(f'# Alternative: load BILD file\n')
                f.write(f'# open "{bild_path}"\n\n')
                
                # viewing settings
                f.write(f'# Viewing settings\n')
                f.write(f'set bgColor white\n')
                f.write(f'lighting soft\n')
                f.write(f'view\n')
            
            print(f"  Written: {cxc_path}")
    
    def generate_all(self, output_dir, diameter_angst):
        """
        Generate all visualization outputs.
        
        Args:
            output_dir: base output directory
            diameter_angst: particle diameter in Angstroms
        """
        output_dir = Path(output_dir)
        
        print("Generating IMOD models (particle radius)...")
        self.write_imod_model(
            output_dir / 'imod_particle_radius',
            diameter_angst,
            color=(0, 255, 0),
            thickness=2
        )
        
        print("\nGenerating IMOD models (center points)...")
        self.write_imod_model(
            output_dir / 'imod_center_points',
            diameter_angst,
            color=(255, 0, 0),
            thickness=4,
            point_mode=True
        )
        
        print("\nGenerating ChimeraX markers...")
        self.write_chimerax_markers(
            output_dir / 'chimerax',
            diameter_angst,
            color=(0, 255, 0, 200)
        )
        
        print("\nGenerating BILD files...")
        self.write_bild(
            output_dir / 'chimerax',
            diameter_angst,
            color=(0, 0.8, 0)
        )
        
        print("\nGenerating ChimeraX scripts...")
        self.write_chimerax_script(
            output_dir / 'chimerax',
            diameter_angst
        )
        
        print(f"\nDone! Outputs in: {output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description='Generate visualization files for particle coordinates'
    )
    parser.add_argument('--candidates', required=True,
                        help='Path to candidates.star (particle list)')
    parser.add_argument('--tomograms', required=True,
                        help='Path to tomograms.star (tomogram metadata)')
    parser.add_argument('--output_dir', required=True,
                        help='Output directory for visualization files')
    parser.add_argument('--diameter', type=float, required=True,
                        help='Particle diameter in Angstroms')
    
    args = parser.parse_args()
    
    viz = ParticleVisualizer(args.candidates, args.tomograms)
    viz.generate_all(args.output_dir, args.diameter)


if __name__ == '__main__':
    main()