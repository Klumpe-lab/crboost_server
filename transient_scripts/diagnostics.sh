# 1. Check rlnTomoHand
cat /users/artem.kushner/dev/crboost_server/projects/ts_ctf_setflip/External/job004/ts_ctf_tilt_series.star

# 2. Compare per-tilt defocus values (sorted) - ours vs GT
echo "=== OURS ===" && \
grep -v "^#\|^$\|^loop\|^_rln\|^data" \
  /users/artem.kushner/dev/crboost_server/projects/ts_ctf_setflip/External/job004/tilt_series/ts_ctf_setflip_Position_1.star \
  | awk '{for(i=1;i<=NF;i++) if($i~/^[0-9]/ && $i+0 > 1000 && $i+0 < 200000) print $i/10000}' \
  | sort -n

echo "=== GT ===" && \
grep -v "^#\|^$\|^loop\|^_rln\|^data" \
  /groups/klumpe/software/Setup/Testing/test1/run12/External/job004/tilt_series/Position_1.star \
  | awk '{for(i=1;i<=NF;i++) if($i~/^[0-9]/ && $i+0 > 1000 && $i+0 < 200000) print $i/10000}' \
  | sort -n

# 3. Compare PyTOM CTF data from job.json - flip_phase, defocus, amplitude_contrast
echo "=== OURS ===" && \
python3 -c "
import json
with open('/users/artem.kushner/dev/crboost_server/projects/ts_ctf_setflip/External/job006/tmResults/ts_ctf_setflip_Position_1_job.json') as f:
    j = json.load(f)
ctf = j.get('ts_metadata', {}).get('ctf_data', [{}])
print('flip_phase:', ctf[0].get('flip_phase'))
print('amplitude_contrast:', ctf[0].get('amplitude_contrast'))
print('voltage:', ctf[0].get('voltage'))
print('defocus sample (first 5):', [round(c.get('defocus',0),4) for c in ctf[:5]])
"

echo "=== GT ===" && \
python3 -c "
import json, glob
p = glob.glob('/groups/klumpe/software/Setup/Testing/test1/run12/External/job006/*/job.json')[0]
with open(p) as f:
    j = json.load(f)
ctf = j.get('ts_metadata', {}).get('ctf_data', [{}])
print('flip_phase:', ctf[0].get('flip_phase'))
print('amplitude_contrast:', ctf[0].get('amplitude_contrast'))
print('voltage:', ctf[0].get('voltage'))
print('defocus sample (first 5):', [round(c.get('defocus',0),4) for c in ctf[:5]])
"

# 4. Compare PyTOM command parameters
echo "=== OURS ===" && \
grep -E "pytom_match|angular|whiten|dose|phase|ctf_model|bandpass|split" \
  /users/artem.kushner/dev/crboost_server/projects/ts_ctf_setflip/External/job006/run.out

echo "=== GT ===" && \
grep -E "pytom_match|angular|whiten|dose|phase|ctf_model|bandpass|split" \
  /groups/klumpe/software/Setup/Testing/test1/run12/External/job006/run.out

# 5. Compare score map statistics
echo "=== OURS score map ===" && \
python3 -c "
import mrcfile, numpy as np
with mrcfile.open('/users/artem.kushner/dev/crboost_server/projects/ts_ctf_setflip/External/job006/tmResults/ts_ctf_setflip_Position_1_scores.mrc', mode='r') as m:
    d = m.data
    print(f'min={d.min():.4f} max={d.max():.4f} mean={d.mean():.4f} std={d.std():.4f}')
    print(f'top 10 values:', sorted(d.flatten())[-10:])
"

echo "=== GT score map ===" && \
python3 -c "
import mrcfile, numpy as np, glob
p = glob.glob('/groups/klumpe/software/Setup/Testing/test1/run12/External/job006/*/*_scores.mrc')[0]
with mrcfile.open(p, mode='r') as m:
    d = m.data
    print(f'min={d.min():.4f} max={d.max():.4f} mean={d.mean():.4f} std={d.std():.4f}')
    print(f'top 10 values:', sorted(d.flatten())[-10:])
"
