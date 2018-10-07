import matplotlib.pyplot as plt
import os.path
from matplotlib import cm


class ChartClient:
    @staticmethod
    def savefig(dir, file_name):
        if not os.path.exists(dir):
            os.mkdir(dir)
        plt.savefig(dir + file_name, bbox_inches="tight")

    @staticmethod
    def closefig(close_type="all"):
        plt.close(close_type)

    @staticmethod
    def plotfig():
        plt.show()

    @staticmethod
    def df_plotfig(df, subplots=False):
        df.plot(subplots=subplots)

    @staticmethod
    def create_pie_chart(df, amount_col, sort_columns=False):
        df.plot(kind='pie', y=amount_col, sort_columns=sort_columns)
        # plt.title('円グラフ', size=16, fontproperties=fp)

    def time_series_graph(self,df, amount_cols_li,does_save=False,file_path=None,figsize=(16, 4), alpha=0.5):
        # 時系列カラムをインデックスに指定する必要がある
        df.plot(y=amount_cols_li, figsize=figsize, alpha=alpha)
        if does_save:
            self.savefig( os.path.split(file_path)[0]+'/', os.path.split(file_path)[1])
        self.closefig()

    @staticmethod
    def plot_x_y(df, x, y, tittle, needsSave=False, file_path=None):
        plt.plot(df[x], df[y], label=tittle)
        plt.legend()
        plt.show()
        if needsSave:
            dir_pair = os.path.split(file_path)
            ChartClient.savefig(dir_pair[0], dir_pair[1])

    def plot_2axis(self,df1,df2, needsSave=False, file_path=None):
        fig, ax1 = plt.subplots()
        ax1.plot(df1['売価'],color='red')
        ax2 = ax1.twinx()  # 2つのプロットを関連付ける
        ax2.bar(df2.index,df2['販売数'])
        # plt.show()
        if needsSave:
            dir_pair = os.path.split(file_path)
            self.savefig(dir_pair[0]+'/', dir_pair[1])
        self.closefig()

    @staticmethod
    def plot_axis_is_index(df, needsSave=False, file_path=None):
        # df.plot(subplots=True,grid=True,colormap='Accent',legend=True,alpha=0.5,layout=(2, 2))
        cols = ['H.伝票金額', 'H.客数（合計）', 'avg_H.伝票金額', 'avg_H.客数（合計）']
        for idx, c in enumerate(cols):
            exec('s_' + str(idx + 1) + '= df[' + '\"' + c + '\"' + ']')

        fig = plt.figure()
        # figureオブジェクトに属するaxesオブジェクトを生成
        ax1 = fig.add_subplot(211)
        ax3 = fig.add_subplot(212)

        ax2 = ax1.twinx()
        ax4 = ax3.twinx()
        all_ax = [ax1, ax2, ax3, ax4]

        for idx, (ax, c) in enumerate(zip(all_ax, cols)):
            if (idx + 1) % 2 == 1:
                exec('ax' + '.plot(s_' + str(idx + 1) + ',' + 'color=\"C0\", label=' + '\"' + c + '\"' + ')')
            else:
                exec('ax' + '.plot(s_' + str(idx + 1) + ',' + 'color=\"C1\", label=' + '\"' + c + '\"' + ')')
        [ax.legend(loc='upper right') if idx+1 % 2 == 1 else ax.legend(loc='upper left') for idx, ax in enumerate(all_ax)]

        # for idx, ax in enumerate(all_ax):
        #     exec('lines_' + str(idx + 1) + ', labels_' + str(idx + 1) + '= ax.get_legend_handles_labels()')
        # for idx, ax in enumerate(all_ax):
        #     ax1.plot((l1[0], l1[0]), ("line1", "line2")
        # [exec('ax' + '.legend([p_' + str(idx + 1) + ',' + 'p_' + str(idx + 3) + ']' + '[' + cols[idx] + ',' + cols[
        #     idx + 2] + '])') for idx, ax in enumerate(all_ax) if idx < 2]

        # ax1.legend([s_1,s_2],['H.伝票金額','H.客数（合計）'])
        # ax2.legend(['H.伝票金額','H.客数（合計）'],['H.伝票金額','H.客数（合計）'])

        # ax1.plot(df_1['H.伝票金額'],color='C0',label=True)
        # ax2.plot(df_1['H.客数（合計）'],color='C1',label=True)
        #
        # ax3.plot(df_2['avg_H.伝票金額'],color='C0',label=True)
        # ax4.plot(df_2['avg_H.客数（合計）'],color='C1',label=True)
        # all_ax = [ax1,ax2,ax3,ax4]
        # [ax.legend() for ax in all_ax]
        #
        # plt.plot(x, y, label='sin')
        # plt.plot(x, y2, label='cos')
        # plt.legend()
        # plt.ylabel('ｙ軸')
        #
        # plt.hist(y, label='y')
        # plt.legend()
        # plt.savefig('matplot_a4.pdf')
        #
        # fig, ax1 = plt.subplots()
        # ax1 = fig.add_subplot(211)
        # ax2 = ax1.twinx()  # 2つのプロットを関連付ける
        #
        # ax3 = fig.add_subplot(212)
        # ax4 = ax3.twinx()
        #
        # ax1.legend()
        # ax2.legend()
        # ax3.legend()
        # ax4.legend()
        #
        # df_1.plot(grid=True,colormap='Accent',legend=True,alpha=0.5)
        # df_2.plot(grid=True,colormap='Accent',legend=True,alpha=0.5)
        if needsSave:
            dir_pair = os.path.split(file_path)
            ChartClient.savefig(dir_pair[0] + '/', dir_pair[1])
        else:
            plt.show()
