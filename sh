python -c "
import starfile
df = starfile.read('projects/zval_fixes/External/job007/tmResults/zval_fixes_Position_1_particles.star')
print(df['rlnTemplateMatchingScore'].describe())
"


python -c "
import mrcfile, numpy as np
with mrcfile.open('projects/zval_fixes/External/job007/tmResults/zval_fixes_Position_1_scores.mrc') as m:
    d = m.data
    print('max:', d.max(), 'mean:', d.mean(), 'std:', d.std())
"
