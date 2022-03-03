import pandas as pd
import matplotlib.pyplot as plot
import seaborn as sns
import numpy as np


class Utils():
    
    @staticmethod
    def generate_stats_graph(result_file, stats_file):

        df = pd.read_csv(result_file)
            
        fig, axes = plot.subplots(1, 2, figsize=(12, 5), sharey=False)
        
        #generate response time distribution
        kwargs = dict(element='step',shrink=.8, alpha=0.6, fill=True, legend=True) 
        ax = sns.histplot(ax=axes[0],data=df,**kwargs)
        #ax.set(xlim=(0.00,1.00)) #set the ylim boundary if auto option is not what you need
        ax.set_title('Response Time Distribution')
        ax.set_xlabel('Response Time (s)')
        ax.set_ylabel('Request Count')
        
        
        #generate percentile distribution       
        summary = np.round(df.describe(percentiles=[0.0, 0.1, 0.2,
                                                        0.3, 0.4, 0.5,
                                                        0.6, 0.7, 0.8,  
                                                        0.9, 0.95, 0.99, 1]),2) # add 1 in the percentile
        dropping = ['count', 'mean', 'std', 'min','max'] #remove metrics not needed for percentile graph
        
        for drop in dropping:
            summary = summary.drop(drop)
        ax = sns.lineplot(ax=axes[1],data=summary,dashes=False, legend=True)
        ax.legend(fontsize='medium')
        #ax.set(ylim=(0.0,1.0)) #set the ylim boundary if auto option is not what you need
        ax.set_title('Percentile Distribution')
        ax.set_xlabel('Percentile')
        ax.set_ylabel('Response Time (s)')
        
        fig.tight_layout(pad=1)
        plot.savefig(stats_file)