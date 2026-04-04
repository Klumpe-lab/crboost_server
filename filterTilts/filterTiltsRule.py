# automatically exclude tilts based on parameters determined in motioncorr and ctffind (before ML)
from src.filterTilts.libFilterTilts import plotFilterTiltsResults

def filterTiltsRule(ts,filterParamRule,outputFolder,plot=None):
   
    #plotFilterTiltsResults(ts,outputFolder,plot)    
    ts.filterTilts(filterParamRule)
    return ts
    

def calcDiffernceToAvgTilt(ts):
    return ts
