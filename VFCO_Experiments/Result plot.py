import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import re

# Set matplotlib parameters for better quality
plt.rcParams['figure.dpi'] = 450
plt.rcParams['savefig.dpi'] = 450

class ExperimentPlotter:
    def __init__(self, csv_dir: str, output_dir: str):
        """
        Initialize the plotter with directories containing CSV files.
        
        Args:
            csv_dir: Directory containing the CSV files from the extractor
            output_dir: Directory to save the generated figures
        """
        self.csv_dir = Path(csv_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Define colors, markers, and linestyles
        self.colors = ['#0B132B', '#1C3F94', '#1565C0', '#1E88E5', '#29B6F6', '#81D4FA']
        self.markers = ['o', 'x', 's', '^', 'D', 'v']
        self.linestyles = ['--', '--', '--', '--', '--', '--']
        
        # Bar graph hatches
        self.hatches = ['||', '--', '||', '--', '||', '--']
        
        # Define display names for policies
        self.policy_display_names = {
            "Low-Freq": "Low-Freq",
            "Avg Freq": "Avg Freq",
            "High Freq": "High Freq",
            "Event Triggered": "Event Triggered",
            "VFCO": "VFCO"
        }
        
        # Define parameter display names
        self.param_display_names = {
            "Corruption Ratio": "Corruption Ratio (%)",
            "Data Scale": "Data Scale",
            "CPU Utilization": "CPU Utilization",
            "Active Users": "Active Users",
            "Arrival Rate": "Arrival Rate (req/s)"
        }
        
        # Define metric display names
        self.metric_display_names = {
            "CST": "CST (s)",
            "USDT": "USDT (s)",
            "Resource_Consumption_Comp_cost": "Computation Cost (s)",
            "Resource_Consumption_Comn_cost": "Communication Cost (KB)"
        }
        
    def read_csv_file(self, filename: str) -> pd.DataFrame:
        """
        Read a CSV file from the csv_dir.
        
        Args:
            filename: Name of the CSV file
            
        Returns:
            DataFrame with the CSV contents
        """
        filepath = self.csv_dir / filename
        if not filepath.exists():
            print(f"⚠️ File not found: {filepath}")
            return None
        
        df = pd.read_csv(filepath, index_col=0)
        return df
    
    def plot_line_graph(self, df: pd.DataFrame, title: str, x_label: str, y_label: str, 
                        save_name: str, use_log_scale: bool = True):
        """
        Create a line graph from the dataframe.
        
        Args:
            df: DataFrame with policies as rows and parameter values as columns
            title: Title of the graph
            x_label: Label for x-axis
            y_label: Label for y-axis
            save_name: Filename to save the figure
            use_log_scale: Whether to use logarithmic scale on y-axis
        """
        plt.figure(figsize=(5, 3.5))
        
        if use_log_scale:
            plt.yscale('log')
        
        # Extract x-axis labels (column names)
        x_labels = [float(col) for col in df.columns]
        
        # Plot each policy (row)
        for i, (policy, row) in enumerate(df.iterrows()):
            data = row.values
            plt.plot(
                x_labels,
                data,
                label=policy,
                color=self.colors[i % len(self.colors)],
                marker=self.markers[i % len(self.markers)],
                linestyle=self.linestyles[i % len(self.linestyles)],
                linewidth=2,
                markersize=6
            )
        
        plt.xlabel(x_label, fontsize=13)
        plt.ylabel(y_label, fontsize=13)
        plt.xticks(fontsize=11)
        plt.yticks(fontsize=11)
        plt.grid(True, which="both", linestyle='--', linewidth=0.5, alpha=0.7)
        plt.legend(fontsize=10, loc='best')
        plt.tight_layout()
        
        # Save figure
        save_path = self.output_dir / f"{save_name}.png"
        plt.savefig(save_path, dpi=450, bbox_inches='tight')
        plt.close()
        print(f"✅ Saved line graph: {save_path}")
    
    def plot_bar_graph(self, df: pd.DataFrame, title: str, x_label: str, y_label: str, 
                       save_name: str, use_log_scale: bool = True):
        """
        Create a bar graph from the dataframe.
        
        Args:
            df: DataFrame with policies as rows and parameter values as columns
            title: Title of the graph
            x_label: Label for x-axis
            y_label: Label for y-axis
            save_name: Filename to save the figure
            use_log_scale: Whether to use logarithmic scale on y-axis
        """
        plt.figure(figsize=(5, 3.5))
        
        if use_log_scale:
            plt.yscale('log')
        
        # Extract x-axis labels (parameter values)
        x_labels = [str(col) for col in df.columns]
        x = np.arange(len(x_labels))
        
        # Bar width
        bar_width = 0.15
        
        # Plot each policy (row) as separate bars
        for i, (policy, row) in enumerate(df.iterrows()):
            data = row.values
            plt.bar(
                x + (i - len(df.index)/2) * bar_width + bar_width/2,
                data,
                width=bar_width,
                label=policy,
                color=self.colors[i % len(self.colors)],
                edgecolor='black',
                linewidth=0.5
            )
        
        plt.xlabel(x_label, fontsize=13)
        plt.ylabel(y_label, fontsize=13)
        plt.xticks(x, x_labels, fontsize=11)
        plt.yticks(fontsize=11)
        plt.legend(fontsize=10, loc='best')
        plt.grid(True, which="both", linestyle='--', linewidth=0.5, alpha=0.7, axis='y')
        plt.tight_layout()
        
        # Save figure
        save_path = self.output_dir / f"{save_name}.png"
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✅ Saved bar graph: {save_path}")
    
    def generate_all_figures(self, figure_type: str = 'line'):
        """
        Generate all 16 figures based on the CSV files.
        
        Args:
            figure_type: Either 'line' or 'bar' for the type of graph
        """
        print("=" * 80)
        print(f"Generating 16 {figure_type.upper()} graphs...")
        print("=" * 80)
        
        # Define all figure specifications
        figures = [
            # CST figures
            {
                'csv_file': 'CST_Corruption_Ratio_OR-EDI.csv',
                'title': 'CST vs Corruption Ratio (OR-EDI)',
                'x_label': self.param_display_names['Corruption Ratio'],
                'y_label': self.metric_display_names['CST'],
                'save_name': 'Figure_01_CST_Corruption_Ratio_OR-EDI',
                'use_log': True
            },
            {
                'csv_file': 'CST_Corruption_Ratio_DL-EDI.csv',
                'title': 'CST vs Corruption Ratio (DL-EDI)',
                'x_label': self.param_display_names['Corruption Ratio'],
                'y_label': self.metric_display_names['CST'],
                'save_name': 'Figure_02_CST_Corruption_Ratio_DL-EDI',
                'use_log': True
            },
            {
                'csv_file': 'CST_CPU_Utilization_OR-EDI.csv',
                'title': 'CST vs CPU Utilization (OR-EDI)',
                'x_label': self.param_display_names['CPU Utilization'],
                'y_label': self.metric_display_names['CST'],
                'save_name': 'Figure_03_CST_CPU_Utilization_OR-EDI',
                'use_log': True
            },
            {
                'csv_file': 'CST_CPU_Utilization_DL-EDI.csv',
                'title': 'CST vs CPU Utilization (DL-EDI)',
                'x_label': self.param_display_names['CPU Utilization'],
                'y_label': self.metric_display_names['CST'],
                'save_name': 'Figure_04_CST_CPU_Utilization_DL-EDI',
                'use_log': True
            },
            
            # USDT figures
            {
                'csv_file': 'USDT_Data_Scale_OR-EDI.csv',
                'title': 'USDT vs Data Scale (OR-EDI)',
                'x_label': self.param_display_names['Data Scale'],
                'y_label': self.metric_display_names['USDT'],
                'save_name': 'Figure_05_USDT_Data_Scale_OR-EDI',
                'use_log': True
            },
            {
                'csv_file': 'USDT_Data_Scale_DL-EDI.csv',
                'title': 'USDT vs Data Scale (DL-EDI)',
                'x_label': self.param_display_names['Data Scale'],
                'y_label': self.metric_display_names['USDT'],
                'save_name': 'Figure_06_USDT_Data_Scale_DL-EDI',
                'use_log': True
            },
            {
                'csv_file': 'USDT_Active_Users_OR-EDI.csv',
                'title': 'USDT vs Active Users (OR-EDI)',
                'x_label': self.param_display_names['Active Users'],
                'y_label': self.metric_display_names['USDT'],
                'save_name': 'Figure_07_USDT_Active_Users_OR-EDI',
                'use_log': True
            },
            {
                'csv_file': 'USDT_Active_Users_DL-EDI.csv',
                'title': 'USDT vs Active Users (DL-EDI)',
                'x_label': self.param_display_names['Active Users'],
                'y_label': self.metric_display_names['USDT'],
                'save_name': 'Figure_08_USDT_Active_Users_DL-EDI',
                'use_log': True
            },
            
            # Resource Consumption (Comp cost) figures
            {
                'csv_file': 'Resource_Consumption_Comp_cost_Data_Scale_OR-EDI.csv',
                'title': 'Computation Cost vs Data Scale (OR-EDI)',
                'x_label': self.param_display_names['Data Scale'],
                'y_label': self.metric_display_names['Resource_Consumption_Comp_cost'],
                'save_name': 'Figure_09_Comp_Cost_Data_Scale_OR-EDI',
                'use_log': True
            },
            {
                'csv_file': 'Resource_Consumption_Comp_cost_Data_Scale_DL-EDI.csv',
                'title': 'Computation Cost vs Data Scale (DL-EDI)',
                'x_label': self.param_display_names['Data Scale'],
                'y_label': self.metric_display_names['Resource_Consumption_Comp_cost'],
                'save_name': 'Figure_10_Comp_Cost_Data_Scale_DL-EDI',
                'use_log': True
            },
            {
                'csv_file': 'Resource_Consumption_Comp_cost_Arrival_Rate_OR-EDI.csv',
                'title': 'Computation Cost vs Arrival Rate (OR-EDI)',
                'x_label': self.param_display_names['Arrival Rate'],
                'y_label': self.metric_display_names['Resource_Consumption_Comp_cost'],
                'save_name': 'Figure_11_Comp_Cost_Arrival_Rate_OR-EDI',
                'use_log': True
            },
            {
                'csv_file': 'Resource_Consumption_Comp_cost_Arrival_Rate_DL-EDI.csv',
                'title': 'Computation Cost vs Arrival Rate (DL-EDI)',
                'x_label': self.param_display_names['Arrival Rate'],
                'y_label': self.metric_display_names['Resource_Consumption_Comp_cost'],
                'save_name': 'Figure_12_Comp_Cost_Arrival_Rate_DL-EDI',
                'use_log': True
            },
            
            # Resource Consumption (Comn cost) figures
            {
                'csv_file': 'Resource_Consumption_Comn_cost_Data_Scale_OR-EDI.csv',
                'title': 'Communication Cost vs Data Scale (OR-EDI)',
                'x_label': self.param_display_names['Data Scale'],
                'y_label': self.metric_display_names['Resource_Consumption_Comn_cost'],
                'save_name': 'Figure_13_Comm_Cost_Data_Scale_OR-EDI',
                'use_log': True
            },
            {
                'csv_file': 'Resource_Consumption_Comn_cost_Data_Scale_DL-EDI.csv',
                'title': 'Communication Cost vs Data Scale (DL-EDI)',
                'x_label': self.param_display_names['Data Scale'],
                'y_label': self.metric_display_names['Resource_Consumption_Comn_cost'],
                'save_name': 'Figure_14_Comm_Cost_Data_Scale_DL-EDI',
                'use_log': True
            },
            {
                'csv_file': 'Resource_Consumption_Comn_cost_Arrival_Rate_OR-EDI.csv',
                'title': 'Communication Cost vs Arrival Rate (OR-EDI)',
                'x_label': self.param_display_names['Arrival Rate'],
                'y_label': self.metric_display_names['Resource_Consumption_Comn_cost'],
                'save_name': 'Figure_15_Comm_Cost_Arrival_Rate_OR-EDI',
                'use_log': True
            },
            {
                'csv_file': 'Resource_Consumption_Comn_cost_Arrival_Rate_DL-EDI.csv',
                'title': 'Communication Cost vs Arrival Rate (DL-EDI)',
                'x_label': self.param_display_names['Arrival Rate'],
                'y_label': self.metric_display_names['Resource_Consumption_Comn_cost'],
                'save_name': 'Figure_16_Comm_Cost_Arrival_Rate_DL-EDI',
                'use_log': True
            }
        ]
        
        successful = 0
        for i, fig_spec in enumerate(figures, 1):
            print(f"\n[{i}/16] Creating figure: {fig_spec['save_name']}")
            print(f"   Reading: {fig_spec['csv_file']}")
            
            # Read CSV file
            df = self.read_csv_file(fig_spec['csv_file'])
            
            if df is None:
                print(f"   ❌ Skipping - CSV file not found")
                continue
            
            # Generate the appropriate graph
            if figure_type.lower() == 'line':
                self.plot_line_graph(
                    df=df,
                    title=fig_spec['title'],
                    x_label=fig_spec['x_label'],
                    y_label=fig_spec['y_label'],
                    save_name=fig_spec['save_name'],
                    use_log_scale=fig_spec['use_log']
                )
            elif figure_type.lower() == 'bar':
                self.plot_bar_graph(
                    df=df,
                    title=fig_spec['title'],
                    x_label=fig_spec['x_label'],
                    y_label=fig_spec['y_label'],
                    save_name=fig_spec['save_name'],
                    use_log_scale=fig_spec['use_log']
                )
            else:
                print(f"   ❌ Invalid figure type. Use 'line' or 'bar'")
                return
            
            successful += 1
        
        print("\n" + "=" * 80)
        print(f"✅ Successfully generated {successful}/16 figures!")
        print(f"📁 Output directory: {self.output_dir}")
    
    def generate_specific_figure(self, figure_name: str, figure_type: str = 'line'):
        """
        Generate a specific figure by name or index.
        
        Args:
            figure_name: Name of the figure (e.g., 'Figure_01_CST_Corruption_Ratio_OR-EDI')
            figure_type: Either 'line' or 'bar'
        """
        # Define all figure specifications (same as above)
        figures = {
            'Figure_01_CST_Corruption_Ratio_OR-EDI': {
                'csv_file': 'CST_Corruption_Ratio_OR-EDI.csv',
                'title': 'CST vs Corruption Ratio (OR-EDI)',
                'x_label': self.param_display_names['Corruption Ratio'],
                'y_label': self.metric_display_names['CST'],
                'use_log': True
            },
            'Figure_02_CST_Corruption_Ratio_DL-EDI': {
                'csv_file': 'CST_Corruption_Ratio_DL-EDI.csv',
                'title': 'CST vs Corruption Ratio (DL-EDI)',
                'x_label': self.param_display_names['Corruption Ratio'],
                'y_label': self.metric_display_names['CST'],
                'use_log': True
            },
            # Add other figures as needed
        }
        
        if figure_name not in figures:
            print(f"❌ Figure '{figure_name}' not found!")
            print(f"Available figures: {list(figures.keys())}")
            return
        
        fig_spec = figures[figure_name]
        
        print(f"\n📊 Creating figure: {figure_name}")
        df = self.read_csv_file(fig_spec['csv_file'])
        
        if df is None:
            print(f"❌ CSV file not found: {fig_spec['csv_file']}")
            return
        
        if figure_type.lower() == 'line':
            self.plot_line_graph(
                df=df,
                title=fig_spec['title'],
                x_label=fig_spec['x_label'],
                y_label=fig_spec['y_label'],
                save_name=figure_name,
                use_log_scale=fig_spec['use_log']
            )
        elif figure_type.lower() == 'bar':
            self.plot_bar_graph(
                df=df,
                title=fig_spec['title'],
                x_label=fig_spec['x_label'],
                y_label=fig_spec['y_label'],
                save_name=figure_name,
                use_log_scale=fig_spec['use_log']
            )


def main():
    """
    Main function to generate the figures.
    """
    # ============================================
    # CONFIGURATION - MODIFY THESE PATHS AS NEEDED
    # ============================================
    
    # Directory containing the CSV files from the extractor
    csv_directory = str(Path(__file__).resolve().parent / "figure_csv")
    
    # Directory to save the generated figures
    output_directory = str(Path(__file__).resolve().parent / "plots")
    
    # Choose figure type: 'line' for line graphs, 'bar' for bar graphs
    FIGURE_TYPE = 'line'  # Change to 'bar' if you want bar graphs
    
    # Choose which figures to generate:
    # Option 1: Generate all 16 figures
    GENERATE_ALL = True
    
    # Option 2: Generate specific figures (if GENERATE_ALL is False)
    SPECIFIC_FIGURES = [
        'Figure_01_CST_Corruption_Ratio_OR-EDI',
        'Figure_02_CST_Corruption_Ratio_DL-EDI',
        # Add more figure names as needed
    ]
    
    # ============================================
    # END OF CONFIGURATION
    # ============================================
    
    print("=" * 80)
    print("Edge Data Integrity Verification - Figure Generator")
    print("=" * 80)
    print(f"\n📂 Looking for CSV files in:")
    print(f"   {csv_directory}")
    print(f"\n💾 Figures will be saved to:")
    print(f"   {output_directory}")
    print(f"\n📊 Figure type: {FIGURE_TYPE.upper()}")
    
    # Check if CSV directory exists
    if not os.path.exists(csv_directory):
        print(f"\n❌ Error: CSV directory does not exist!")
        print(f"   Please run the data extraction script first to generate CSV files.")
        return
    
    # Create plotter instance
    plotter = ExperimentPlotter(csv_directory, output_directory)
    
    # Generate figures based on configuration
    if GENERATE_ALL:
        plotter.generate_all_figures(figure_type=FIGURE_TYPE)
    else:
        for figure_name in SPECIFIC_FIGURES:
            plotter.generate_specific_figure(figure_name, figure_type=FIGURE_TYPE)
    
    print("\n✅ Figure generation complete!")


if __name__ == "__main__":
    main()