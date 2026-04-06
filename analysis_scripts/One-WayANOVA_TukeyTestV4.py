#Author: Mason Allen
#Created On: 2_16_2026
#Last Updated: 2_16_2026
#Description: Performs ANOVA on multi-condition response data. If null is rejected, script performs Tukey pairwise comparison test
#and prints results

import numpy as np
from scipy import stats
import pandas as pd
from tkinter import filedialog as fd
from scipy.stats import f_oneway
import matplotlib.pyplot as plt
from statsmodels.stats.multicomp import pairwise_tukeyhsd
import seaborn as sns 
import matplotlib.pyplot as plt
from datetime import datetime


#defines Alpha value
alpha = 0.05

#prompts user with a file selection window to choose a CSV. Converts CSV to a long dataframe format
def open_file_selection():
    
    #try opening file and converting to df. Throw error message on exception 
    try:
        file_path = fd.askopenfile()
        
        #if no file is selected, quit the script
        if file_path == None:
            print("No file sected, exiting script...")
            exit()

        df = pd.read_csv(file_path)

        #clean all non ACII characters
        df.columns = df.columns.str.replace(r"[^\x20-\x7E]", "", regex=True)

        #DEBUG
        #print(df)
        #print()

    except ImportError:
        print("Please install openpyxl: pip install openpyxl")
        exit()

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        exit()
    
    #return the long format dataframe
    return df

#create a table of statistics for each condition group and print these
def analyze_dataset(df):
    
    #create stats table, using GroupBy to group by condition
    stats_tbl = ( 
        df.groupby("Group") 
            .agg( 
                mean_value=("Value", "mean"), 
                median_value=("Value", "median"), 
                std_dev=("Value", "std"), 
                n_observations=("Value", "size") 
            ) 
            .reset_index()
            .sort_values("Group")
    )
    
    # Convert DataFrame to string without the default index
    table_str = stats_tbl.to_string(index=False)

    # Extract the header line (first line of the string)
    header_line = table_str.split("\n")[0]

    # Create decoration lines matching header width
    top_line = "=" * len(header_line)
    bottom_line = "-" * len(header_line)

    #display stats table
    print("STATISTICS SUMMARY TABLE (BY CONDITION):")
    print()
    print(top_line)
    print(header_line)
    print(bottom_line)
    print("\n".join(table_str.split("\n")[1:]))
    print()


#performs ANOVA test on df to determine if there are any significant differences in the means across all groups. Prints results.
def perform_ANOVA(df,alpha):
    
    #creates arrays for each condition group. Arrays hold the set of the response values for each 
    groups = [group_df["Value"].values for _, group_df in df.groupby("Group")]

    #perform one-way ANOVA on all response values
    ANOVA_results = f_oneway(*groups) #CHECK to see if equal population variances should be assumed 

    #print results of ANOVA
    print("ANOVA RESULTS:")
    print()
    print("p-value:",ANOVA_results.pvalue)#round(ANOVA_results.pvalue,10))
    print("F Statistic:",round(ANOVA_results.statistic,10))

    if ANOVA_results.pvalue > alpha:
        inequality = "GREATER THAN"
        significance = "NOT STATISTICALLY SIGNIFICANT"
        ANOVA_sig_effect = False

    else:
        inequality = "LESS THAN OR EQUAL TO"
        significance = "STATISTICALLY SIGNIFICANT"
        ANOVA_sig_effect = True

    print("reject null:",ANOVA_sig_effect)
    print()
    print("description: p-value of",round(ANOVA_results.pvalue,3),"is",inequality,"alpha value of",alpha)
    print("Difference of means is",significance)
    print()

    #return T/F value depending on if the null is rejected or not
    return ANOVA_sig_effect

#performs Tukey test on all pairs of conditions to determine which pairs have significant differences in their means.
#Prints results, generates visualizations, and creates a CSV output  
def perform_Tukey(df,alp):
    
    #perform Tukey pairwise comparisons with alpha specified above
    Tukey_results = pairwise_tukeyhsd(endog=df["Value"], groups=df["Group"], alpha=alp)
    
    #display Tukey results
    print("TUKEY POST HOC PAIRWISE TEST RESULTS:")
    print()
    print(Tukey_results)

    #create dataframe for Tukey results
    tukey_df = pd.DataFrame(
        Tukey_results._results_table.data[1:],   # all rows except header
        columns=Tukey_results._results_table.data[0]  # header row
        )

    #initialize effect size array
    effect_size_arr = []

    #for each pairwise comparison, calculate effect size
    for i, row in tukey_df.iterrows():
        #if row["reject"] == True: #use if reconfiguring CSV to only display rejected null pairs

        #identify condition names
        group1 = row["group1"]
        group2 = row["group2"]
        
        #create arrays of response values for identified condition names
        arr1 = df.loc[df["Group"] == group1, "Value"].values
        arr2 = df.loc[df["Group"] == group2, "Value"].values
        
        #add calculated effect size to effect size array
        effect_size_arr.append(cohensd(arr1,arr2))

        #DEBUG
        #print("effect size array:", effect_size_arr)

    #add the effect size array onto the Tukey results dataframe as a column
    tukey_df["effectsize(CohensD)"] = effect_size_arr

    #grab current datatime to add to csv title
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    title = f"tukey_results{current_datetime}.csv"

    #create or update Tukey results csv for easy viewing
    tukey_df.to_csv(title, index=False)

    #create visualizations for additional analysis
    #create dual axis plot to mount boxplot and swarmplot next to eachother
    
    #DUAL PLOTS
    ########################
    #fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(14, 7))

    #creates boxplots of response values by condition
    #sns.boxplot(data=df, x="Group", y="Value", hue = "Group", ax=axes[0]) 
    #axes[0].set_ylabel("Strength (N/m)")
    #axes[0].set_xlabel("Condition")  
    #axes[0].set_title("Soil Stength by Condition (box plot)") 
    #axes[0].grid(True, which='both', axis='y', linestyle='-', linewidth=0.5)

    #creates swarm plot of response values by condition
    #sns.swarmplot(data=df, x="Group", y="Value", hue = "Group", ax=axes[1]) 
    #axes[1].set_ylabel("Strength (N/m)")
    #axes[1].set_xlabel("Condition")  
    #axes[1].set_title("Soil Stength by Condition (swarm chart)")
    #axes[1].grid(True, which='both', axis='y', linestyle='-', linewidth=0.5)
    
    #display plots
    #plt.tight_layout()
    #plt.show()
    #####################


    #OVERLAYED PLOTS
    # create figure and single axis
    fig, ax = plt.subplots(figsize=(8, 6))

    # create boxplot
    sns.boxplot(data=df, x="Group", y="Value", hue="Group", ax=ax, dodge=False, showcaps=True, boxprops={'zorder': 1, 'alpha':0.6})

    # overlay swarmplot (with translucency)
    sns.swarmplot(
        data=df, 
        x="Group", 
        y="Value", 
        hue="Group", 
        dodge=False, 
        ax=ax, 
        alpha=0.6,    # transparency
        zorder=2      # ensure it sits above boxplot
    )

    # clean up duplicate legends from overlapping hue layers
    handles, labels = ax.get_legend_handles_labels()
    #ax.legend(handles[:len(set(df["Group"]))], labels[:len(set(df["Group"]))], title="Group")

    # axis labels and title
    ax.set_ylabel("Strength (N/m)")
    ax.set_xlabel("Condition")
    ax.set_title("Soil Strength by Condition (Box + Swarm Overlay)")

    # grid and layout
    ax.grid(True, which='both', axis='y', linestyle='-', linewidth=0.5)
    plt.tight_layout()
    plt.show()

    
    #create and show a confidence interval plot
    Tukey_results.plot_simultaneous(figsize=(10,2.5))
    plt.grid(True, which='both', axis='x', linestyle='-', linewidth=0.5)
    plt.show()

#calculates Cohen's D effect size
def cohensd(arr1,arr2):
    
    #Source: https://www.campbellcollaboration.org/calculator/equations

    #calc sample sizes
    n1 = len(arr1)
    n2 = len(arr2)

    #calc variance
    var1 = np.var(arr1, ddof=1)
    var2 = np.var(arr2, ddof=1)

    #calc means
    m1 = np.mean(arr1)
    m2 = np.mean(arr2)

    #calc pooled standard deviation
    pooled_std = np.sqrt(((n1 - 1) * var1 + (n2 - 1) * var2) / (n1 + n2 - 2))

    #return Cohen's D effect size
    return (m1-m2)/pooled_std

#### MAIN #####

df = open_file_selection()

analyze_dataset(df)

significant = perform_ANOVA(df,alpha)

if significant == True:
    perform_Tukey(df,alpha)
