python3 -c "
import glob, xml.etree.ElementTree as ET, os, statistics
# project_name='all_together'
project_name='auto_flip_cusesum'
# project_name='442split_pytom'

xmls = sorted(glob.glob('/users/artem.kushner/dev/crboost_server/projects/{}/External/job002/warp_frameseries/*.xml'.format(project_name)))
vals = []

for f in xmls:
    root = ET.parse(f).getroot()
    ctf = root.find('.//Param[@Name=\"Defocus\"]')
    if ctf is not None:
        vals.append((os.path.basename(f), float(ctf.attrib['Value'])))

print(f'n={len(vals)}')
for name, v in vals:
    print(f'  {name}: {v:.4f}')
if len(vals) > 1:
    vs = [v for _,v in vals]
    print(f'mean={statistics.mean(vs):.3f} std={statistics.stdev(vs):.4f} min={min(vs):.3f} max={max(vs):.3f}')
"
