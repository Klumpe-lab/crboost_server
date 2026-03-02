# TM underpicking...

The factors i can see being different for now are only the following:

- c_use_sum during MotionCorrection (i have this one as True just because otherwise for whatever reason my aretomo yields completely shitty and spread out defocus values for all tilts after the first 5-6 -- with this parameter i can pretty faithfully land in the ~5.1u range that GT has). This is extremely perplexing to me.

- i used `2:2:1` for my pytom splits just because it runs quicker. I tried running with 4:4:2 split and it didn't change anything anyway

- my template are sliiightly different because i use a slightly different mehtod of generating them, but not nearly enough to account for a 30% underpicking.

- I guess i'm also using pytom v0.12 instead of v0.10, but that shouldn't matter, right?


# Next steps

1. Figure out underpicking 
    - increase threshold 
    - failing that, pin v0.10
    - failing that, tighten the templates further
    - failing that, despair

2. Multiple datasets of copia to prototype "merged" particle reconstruct.
4. Differentiate lanes for different particle species/templates
3. Split UI into modes (roughly pre-post reconstruction)
5. define own success conditions per-job
    - stpe1. eliminate reliance on particular package error codes (verify files)
    - step2. per-job diagnostics