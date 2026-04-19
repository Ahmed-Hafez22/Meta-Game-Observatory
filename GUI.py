import sys
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QComboBox, QLabel, QPushButton, QTabWidget, QLineEdit,
    QStatusBar, QMessageBox, QProgressBar, QFrame
)
from PySide6.QtCore import Qt, QTimer, QThread, Signal
import pyqtgraph as pg
from datetime import datetime as dt
from collections import Counter
import connect

db_connection = connect.connect_to_db()
cursor = db_connection.cursor()


class PipelineWorker(QThread):
    finished = Signal(bool, str)
    progress = Signal(int, str)
    
    def __init__(self, game_name):
        super().__init__()
        self.game_name = game_name
    
    def run(self):
        try:
            from pipeline import run_pipeline
            
            self.progress.emit(25, f"Fetching data for {self.game_name}...")
            game_id = run_pipeline(self.game_name)
            
            if game_id and game_id > 0:
                self.progress.emit(100, f"Successfully added {self.game_name}")
                self.finished.emit(True, self.game_name)
            else:
                self.finished.emit(False, "Pipeline returned no game ID")
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            self.finished.emit(False, str(e))


class GameAnalyticsDashboard(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Meta Game Observatory")
        self.setGeometry(100, 100, 1300, 850)
        
        # Central widget and main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(10)
        
        # === Search Bar ===
        search_frame = QFrame()
        search_frame.setFrameStyle(QFrame.StyledPanel)
        search_layout = QHBoxLayout(search_frame)
        search_layout.setContentsMargins(10, 5, 10, 5)
        
        search_icon = QLabel("🔍")
        search_icon.setStyleSheet("font-size: 14px;")
        search_layout.addWidget(search_icon)
        
        search_layout.addWidget(QLabel("Search Game:"))
        
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Enter game name (e.g., Dota 2, Cyberpunk 2077)...")
        self.search_input.setMinimumWidth(350)
        search_layout.addWidget(self.search_input)
        
        self.search_button = QPushButton("Search")
        self.search_button.setStyleSheet("""
            QPushButton {
                background-color: #0078d4;
                color: white;
                padding: 6px 16px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #106ebe;
            }
        """)
        search_layout.addWidget(self.search_button)
        
        search_layout.addStretch()
        main_layout.addWidget(search_frame)
        
        # === Separator Line ===
        separator = QFrame()
        separator.setFrameStyle(QFrame.HLine)
        separator.setStyleSheet("color: #cccccc;")
        main_layout.addWidget(separator)
        
        # === Control Panel ===
        control_panel = QHBoxLayout()
        control_panel.setSpacing(15)
        
        # Developer selector
        control_panel.addWidget(QLabel("Developer:"))
        self.developer_combo = QComboBox()
        self.developer_combo.setMinimumWidth(220)
        self.developer_combo.addItem("-- Select Developer --")
        control_panel.addWidget(self.developer_combo)
        
        # Game selector
        control_panel.addWidget(QLabel("Game:"))
        self.game_combo = QComboBox()
        self.game_combo.setMinimumWidth(280)
        self.game_combo.addItem("-- Select Game --")
        control_panel.addWidget(self.game_combo)
        
        # Refresh button
        self.refresh_btn = QPushButton("⟳ Refresh")
        self.refresh_btn.setStyleSheet("padding: 6px 16px;")
        control_panel.addWidget(self.refresh_btn)
        
        # Last updated label
        self.last_updated_label = QLabel("Last Updated: Never")
        self.last_updated_label.setStyleSheet("color: #666666; font-size: 12px;")
        control_panel.addWidget(self.last_updated_label)
        
        control_panel.addStretch()
        main_layout.addLayout(control_panel)
        
        # === Progress Bar (Hidden by default) ===
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 1px solid #cccccc;
                border-radius: 4px;
                text-align: center;
            }
            QProgressBar::chunk {
                background-color: #0078d4;
                border-radius: 3px;
            }
        """)
        main_layout.addWidget(self.progress_bar)
        
        # === Tab Widget ===
        self.tabs = QTabWidget()
        self.tabs.setStyleSheet("""
            QTabWidget::pane {
                border: 1px solid #cccccc;
                border-radius: 4px;
            }
            QTabBar::tab {
                padding: 8px 20px;
                margin-right: 4px;
            }
            QTabBar::tab:selected {
                background-color: #0078d4;
                color: white;
            }
        """)
        main_layout.addWidget(self.tabs)
        
        # Create tabs
        self.create_overview_tab()
        self.create_developer_tab()
        self.create_patch_analysis_tab()
        self.create_info_tab()
        
        # === Status Bar ===
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready")
        
        # === Connect Signals ===
        self.connect_signals()
        self.load_developers()
        
    def create_overview_tab(self):
        """Player count and review trends tab"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(20)
        
        # === Player Count Chart ===
        player_chart_frame = QFrame()
        player_chart_frame.setFrameStyle(QFrame.StyledPanel)
        player_layout = QVBoxLayout(player_chart_frame)
        
        player_title = QLabel("📊 Player Count Trend")
        player_title.setStyleSheet("font-size: 16px; font-weight: bold; padding: 10px;")
        player_layout.addWidget(player_title)
        
        self.player_chart = pg.PlotWidget()
        self.player_chart.setBackground('#f5f5f5')
        self.player_chart.setMinimumHeight(300)
        self.player_chart.setLabel('left', 'Players')
        self.player_chart.setLabel('bottom', 'Date')
        self.player_chart.showGrid(x=True, y=True, alpha=0.3)
        self.player_chart.addLegend()
        player_layout.addWidget(self.player_chart)
        layout.addWidget(player_chart_frame)
        
        # === Review Score Chart ===
        review_chart_frame = QFrame()
        review_chart_frame.setFrameStyle(QFrame.StyledPanel)
        review_layout = QVBoxLayout(review_chart_frame)
        
        review_title = QLabel("⭐ Review Score History")
        review_title.setStyleSheet("font-size: 16px; font-weight: bold; padding: 10px;")
        review_layout.addWidget(review_title)
        
        self.review_chart = pg.PlotWidget()
        self.review_chart.setBackground('#f5f5f5')
        self.review_chart.setMinimumHeight(250)
        self.review_chart.setLabel('left', 'Score')
        self.review_chart.setLabel('bottom', 'Date')
        self.review_chart.showGrid(x=True, y=True, alpha=0.3)
        self.review_chart.addLegend()
        review_layout.addWidget(self.review_chart)
        layout.addWidget(review_chart_frame)
        
        self.tabs.addTab(tab, "📈 Overview")
    
    def create_developer_tab(self):
        """Developer stats and platform distribution tab"""
        tab = QWidget()
        layout = QHBoxLayout(tab)
        layout.setSpacing(20)
        
        # === Left side: Developer Stats ===
        left_frame = QFrame()
        left_frame.setFrameStyle(QFrame.StyledPanel)
        left_frame.setStyleSheet("background-color: #2b2b2b; border-radius: 8px;")
        left_layout = QVBoxLayout(left_frame)
        
        dev_title = QLabel("🏢 Developer Statistics")
        dev_title.setStyleSheet("font-size: 16px; font-weight: bold; padding: 10px; color: #ffffff;")
        left_layout.addWidget(dev_title)
        
        self.dev_stats_widget = QWidget()
        dev_stats_inner = QVBoxLayout(self.dev_stats_widget)
        dev_stats_inner.setSpacing(15)
        dev_stats_inner.setAlignment(Qt.AlignTop)
        
        self.total_games_label = QLabel("📊 Total Games: --")
        self.total_games_label.setStyleSheet("""
            font-size: 14px; 
            padding: 12px; 
            background-color: #0078d4; 
            border-radius: 6px;
            color: #ffffff;
            font-weight: bold;
        """)
        dev_stats_inner.addWidget(self.total_games_label)
        
        self.total_players_label = QLabel("👥 Current Total Players: --")
        self.total_players_label.setStyleSheet("""
            font-size: 14px; 
            padding: 12px; 
            background-color: #28a745; 
            border-radius: 6px;
            color: #ffffff;
            font-weight: bold;
        """)
        dev_stats_inner.addWidget(self.total_players_label)
        
        self.avg_score_label = QLabel("⭐ Average Review Score: --")
        self.avg_score_label.setStyleSheet("""
            font-size: 14px; 
            padding: 12px; 
            background-color: #ffc107; 
            border-radius: 6px;
            color: #000000;
            font-weight: bold;
        """)
        dev_stats_inner.addWidget(self.avg_score_label)
        
        self.highest_peak_label = QLabel("🏆 All-Time Highest Peak: --")
        self.highest_peak_label.setStyleSheet("""
            font-size: 14px; 
            padding: 12px; 
            background-color: #dc3545; 
            border-radius: 6px;
            color: #ffffff;
            font-weight: bold;
        """)
        dev_stats_inner.addWidget(self.highest_peak_label)
        
        dev_stats_inner.addStretch()
        left_layout.addWidget(self.dev_stats_widget)
        layout.addWidget(left_frame, stretch=1)
        
        # === Right side: Platform Distribution ===
        right_frame = QFrame()
        right_frame.setFrameStyle(QFrame.StyledPanel)
        right_frame.setStyleSheet("background-color: #2b2b2b; border-radius: 8px;")
        right_layout = QVBoxLayout(right_frame)
        
        platform_title = QLabel("🎮 Platform Distribution")
        platform_title.setStyleSheet("font-size: 16px; font-weight: bold; padding: 10px; color: #ffffff;")
        right_layout.addWidget(platform_title)
        
        self.platform_chart = pg.PlotWidget()
        self.platform_chart.setBackground('#3b3b3b')
        self.platform_chart.setMinimumHeight(400)
        right_layout.addWidget(self.platform_chart)
        
        layout.addWidget(right_frame, stretch=1)
        
        self.tabs.addTab(tab, "🏢 Developer Analysis")
    
    def create_patch_analysis_tab(self):
        """Patch impact and history tab"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(20)
        
        # Patch timeline chart
        timeline_frame = QFrame()
        timeline_frame.setFrameStyle(QFrame.StyledPanel)
        timeline_layout = QVBoxLayout(timeline_frame)
        
        timeline_title = QLabel("📅 Patch Timeline with Player Count")
        timeline_title.setStyleSheet("font-size: 16px; font-weight: bold; padding: 10px;")
        timeline_layout.addWidget(timeline_title)
        
        self.patch_chart = pg.PlotWidget()
        self.patch_chart.setBackground('#f5f5f5')
        self.patch_chart.setMinimumHeight(250)
        self.patch_chart.setLabel('left', 'Players')
        self.patch_chart.setLabel('bottom', 'Date')
        self.patch_chart.showGrid(x=True, y=True, alpha=0.3)
        self.patch_chart.addLegend()
        timeline_layout.addWidget(self.patch_chart)
        layout.addWidget(timeline_frame)
        
        # Patch list
        impact_frame = QFrame()
        impact_frame.setFrameStyle(QFrame.StyledPanel)
        impact_layout = QVBoxLayout(impact_frame)
        
        impact_title = QLabel("📋 Recent Patches")
        impact_title.setStyleSheet("font-size: 16px; font-weight: bold; padding: 10px;")
        impact_layout.addWidget(impact_title)
        
        self.patch_list_label = QLabel("Select a game to view patches")
        self.patch_list_label.setStyleSheet("font-size: 13px; padding: 10px;")
        self.patch_list_label.setWordWrap(True)
        self.patch_list_label.setAlignment(Qt.AlignTop)
        self.patch_list_label.setMinimumHeight(150)
        impact_layout.addWidget(self.patch_list_label)
        layout.addWidget(impact_frame)
        
        self.tabs.addTab(tab, "🔧 Patch Analysis")
    
    def create_info_tab(self):
        """Game details and metadata tab"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(15)
        
        # === Game Header ===
        self.game_title_label = QLabel("Select a Game")
        self.game_title_label.setStyleSheet("font-size: 20px; font-weight: bold; padding: 10px; color: #0078d4;")
        self.game_title_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.game_title_label)
        
        # === Info Grid ===
        info_frame = QFrame()
        info_frame.setFrameStyle(QFrame.StyledPanel)
        info_frame.setStyleSheet("background-color: #2b2b2b; border-radius: 8px; padding: 15px;")
        info_layout = QVBoxLayout(info_frame)
        
        # Release Date
        self.release_date_label = QLabel("📅 Release Date: --")
        self.release_date_label.setStyleSheet("color: #ffffff; font-size: 14px; padding: 8px;")
        info_layout.addWidget(self.release_date_label)
        
        # Description
        desc_title = QLabel("📝 Description")
        desc_title.setStyleSheet("color: #ffffff; font-size: 16px; font-weight: bold; padding: 8px;")
        info_layout.addWidget(desc_title)
        
        self.desc_label = QLabel("No description available")
        self.desc_label.setWordWrap(True)
        self.desc_label.setStyleSheet("color: #cccccc; font-size: 13px; padding: 8px; background-color: #3b3b3b; border-radius: 5px;")
        self.desc_label.setMinimumHeight(80)
        info_layout.addWidget(self.desc_label)
        
        layout.addWidget(info_frame)
        
        # === Genres and Publishers Row ===
        row_frame = QFrame()
        row_layout = QHBoxLayout(row_frame)
        row_layout.setSpacing(20)
        
        # Genres
        genre_frame = QFrame()
        genre_frame.setFrameStyle(QFrame.StyledPanel)
        genre_frame.setStyleSheet("background-color: #2b2b2b; border-radius: 8px; padding: 15px;")
        genre_layout = QVBoxLayout(genre_frame)
        
        genre_title = QLabel("🎮 Genres")
        genre_title.setStyleSheet("color: #ffffff; font-size: 16px; font-weight: bold; padding: 8px;")
        genre_layout.addWidget(genre_title)
        
        self.genres_label = QLabel("--")
        self.genres_label.setWordWrap(True)
        self.genres_label.setStyleSheet("color: #cccccc; font-size: 13px; padding: 8px;")
        self.genres_label.setAlignment(Qt.AlignTop)
        genre_layout.addWidget(self.genres_label)
        genre_layout.addStretch()
        
        row_layout.addWidget(genre_frame, stretch=1)
        
        # Publishers
        pub_frame = QFrame()
        pub_frame.setFrameStyle(QFrame.StyledPanel)
        pub_frame.setStyleSheet("background-color: #2b2b2b; border-radius: 8px; padding: 15px;")
        pub_layout = QVBoxLayout(pub_frame)
        
        pub_title = QLabel("🏭 Publishers")
        pub_title.setStyleSheet("color: #ffffff; font-size: 16px; font-weight: bold; padding: 8px;")
        pub_layout.addWidget(pub_title)
        
        self.publishers_label = QLabel("--")
        self.publishers_label.setWordWrap(True)
        self.publishers_label.setStyleSheet("color: #cccccc; font-size: 13px; padding: 8px;")
        self.publishers_label.setAlignment(Qt.AlignTop)
        pub_layout.addWidget(self.publishers_label)
        pub_layout.addStretch()
        
        row_layout.addWidget(pub_frame, stretch=1)
        
        layout.addWidget(row_frame, stretch=1)
        
        self.tabs.addTab(tab, "ℹ️ Game Info")
    
    def connect_signals(self):
        """Connect UI signals to slots"""
        self.search_button.clicked.connect(self.on_search_clicked)
        self.search_input.returnPressed.connect(self.on_search_clicked)
        self.refresh_btn.clicked.connect(self.on_refresh_clicked)
        self.developer_combo.currentIndexChanged.connect(self.on_developer_changed)
        self.game_combo.currentIndexChanged.connect(self.on_game_changed)
    
    # === Signal Handlers ===
    
    def on_search_clicked(self):
        game_name = self.search_input.text().strip()
        if not game_name:
            QMessageBox.warning(self, "Input Required", "Please enter a game name to search.")
            return
        
        self.status_bar.showMessage(f"Searching for '{game_name}'...")
        
        game_info = self.check_game_exists(game_name)
        
        if game_info:
            self.status_bar.showMessage(f"Found: {game_info['title']}")
            self.select_game_in_dropdowns(game_info)
            self.search_input.clear()
        else:
            reply = QMessageBox.question(
                self,
                "Game Not Found",
                f"'{game_name}' is not in the database.\n\nWould you like to add it now?\n\nThis may take 10-30 seconds.",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if reply == QMessageBox.Yes:
                self.add_new_game(game_name)
            else:
                self.status_bar.showMessage("Ready")
                self.search_input.clear()
    
    def on_refresh_clicked(self):
        self.status_bar.showMessage("Refreshing data...")
        self.last_updated_label.setText(f"Last Updated: {dt.now().strftime('%H:%M:%S')}")
        
        game_id = self.game_combo.currentData()
        if game_id:
            self.update_player_chart(game_id)
            self.update_review_chart(game_id)
            self.update_patch_chart(game_id)
            self.update_platform_chart(game_id)
            self.update_info_tab(game_id)
        
        developer_id = self.developer_combo.currentData()
        if developer_id:
            self.update_developer_stats(developer_id)
        
        self.status_bar.showMessage("Ready")
    
    def on_developer_changed(self, index):
        if index > 0:
            developer_id = self.developer_combo.currentData()
            developer_name = self.developer_combo.currentText()
            self.status_bar.showMessage(f"Loading games for {developer_name}...")
            self.load_games_for_developer(developer_id)
            self.update_developer_stats(developer_id)
            self.status_bar.showMessage("Ready")
    
    def on_game_changed(self, index):
        if index > 0:
            game_id = self.game_combo.currentData()
            game_name = self.game_combo.currentText()
            self.status_bar.showMessage(f"Loading data for {game_name}...")
            
            self.update_player_chart(game_id)
            self.update_review_chart(game_id)
            self.update_patch_chart(game_id)
            self.update_platform_chart(game_id)
            self.update_info_tab(game_id)
            
            self.status_bar.showMessage("Ready")
    
    def on_pipeline_progress(self, value, message):
        self.progress_bar.setValue(value)
        self.status_bar.showMessage(message)
    
    def on_pipeline_finished(self, success, message):
        self.progress_bar.setVisible(False)
        self.search_button.setEnabled(True)
        
        if success:
            self.status_bar.showMessage(f"Successfully added {message}")
            self.load_developers()
            
            game_info = self.check_game_exists(message)
            if game_info:
                self.select_game_in_dropdowns(game_info)
        else:
            self.status_bar.showMessage(f"Error: {message}")
            QMessageBox.critical(self, "Pipeline Error", f"Failed to add game:\n\n{message}")
        
        self.search_input.clear()
    
    # === Data Loading Methods ===
    
    def load_developers(self):
        selection_query = """
                            SELECT developer_id, name
                            FROM dim_developer
                            ORDER BY name
                            """ 
        cursor.execute(selection_query)
        developers = cursor.fetchall()

        self.developer_combo.clear()
        self.developer_combo.addItem("-- Select Developer --", None)

        for dev_id, dev_name in developers:
            self.developer_combo.addItem(dev_name, dev_id)
    
    def load_games_for_developer(self, developer_id):
        selection_query = """
                            SELECT g.title, g.game_id
                            FROM dim_game AS g
                            JOIN game_developers AS gd ON g.game_id = gd.game_id
                            WHERE gd.developer_id = %s
                            ORDER BY g.title
                            """
        cursor.execute(selection_query, (developer_id,))
        games = cursor.fetchall()

        self.game_combo.clear()
        self.game_combo.addItem("-- Select Game --", None)

        for game_name, game_id in games:
            self.game_combo.addItem(game_name, game_id)

        self.status_bar.showMessage(f"Loaded {len(games)} games")
    
    def check_game_exists(self, game_name):
        selection_query = """
                            SELECT g.game_id, g.title, d.developer_id, d.name
                            FROM dim_game g
                            LEFT JOIN game_developers gd ON g.game_id = gd.game_id
                            LEFT JOIN dim_developer d ON gd.developer_id = d.developer_id
                            WHERE LOWER(g.title) = LOWER(%s)
                            """
        cursor.execute(selection_query, (game_name,))
        result = cursor.fetchone()
        
        if result:
            return {
                'game_id': result[0],
                'title': result[1],
                'developer_id': result[2],
                'developer_name': result[3]
            }
        return None
    
    def select_game_in_dropdowns(self, game_info):
        for i in range(self.developer_combo.count()):
            if self.developer_combo.itemData(i) == game_info['developer_id']:
                self.developer_combo.setCurrentIndex(i)
                break
        
        for i in range(self.game_combo.count()):
            if self.game_combo.itemData(i) == game_info['game_id']:
                self.game_combo.setCurrentIndex(i)
                break
    
    def add_new_game(self, game_name):
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.search_button.setEnabled(False)
        
        self.worker = PipelineWorker(game_name)
        self.worker.progress.connect(self.on_pipeline_progress)
        self.worker.finished.connect(self.on_pipeline_finished)
        self.worker.start()
    
    def load_player_count_data(self, game_id):
        selection_query = """
                            SELECT pc.peak_players, pc.avg_players, d.full_date
                            FROM fact_player_count AS pc
                            JOIN dim_date AS d ON pc.date_id = d.date_id
                            WHERE game_id = %s
                            ORDER BY pc.recorded_at
                            """
        cursor.execute(selection_query, (game_id,))
        player_count = cursor.fetchall()

        dates = []
        peak_players = []
        avg_players = []

        for peak_player, avg_player, full_date in player_count:
            peak_players.append(peak_player)
            avg_players.append(avg_player)
            dates.append(full_date)

        return dates, peak_players, avg_players
    
    def load_review_data(self, game_id):
        selection_query = """
                            SELECT r.sentiment, r.total_reviews, r.score, d.full_date
                            FROM fact_reviews AS r
                            JOIN dim_date AS d ON r.date_id = d.date_id
                            WHERE game_id = %s
                            ORDER BY d.full_date
                            """
        cursor.execute(selection_query, (game_id,))
        reviews = cursor.fetchall()

        scores = []
        total_reviews = []
        sentiment = []
        dates = []

        for sentim, total, score, date in reviews:
            scores.append(score)
            total_reviews.append(total)
            sentiment.append(sentim)
            dates.append(date)

        return scores, total_reviews, sentiment, dates
    
    def load_patch_data(self, game_id):
        selection_query = """
                            SELECT p.version, p.patch_type, d.full_date
                            FROM fact_patch_events AS p
                            JOIN dim_date AS d ON p.date_id = d.date_id
                            WHERE game_id = %s
                            ORDER BY d.full_date
                            """
        cursor.execute(selection_query, (game_id,))
        patches = cursor.fetchall()

        versions = []
        patch_types = []
        dates = []

        for ver, ptype, date in patches:
            versions.append(ver)
            patch_types.append(ptype)
            dates.append(date)
        
        return versions, patch_types, dates
    
    def load_platform_data(self, game_id):
        selection_query = """
                            SELECT p.name
                            FROM dim_platform AS p
                            JOIN game_platform AS gp ON p.platform_id = gp.platform_id
                            WHERE gp.game_id = %s
                            ORDER BY p.name
                            """
        cursor.execute(selection_query, (game_id,))
        platforms = [row[0] for row in cursor.fetchall()]
        return platforms
    
    def load_developer_stats(self, developer_id):
        stats = {}
        
        cursor.execute("""
                        SELECT COUNT(*)
                        FROM game_developers
                        WHERE developer_id = %s
                        """, (developer_id,))
        stats['total_games'] = cursor.fetchone()[0] or 0
        
        try:
            cursor.execute("""
                            SELECT COALESCE(SUM(pc.current_players_count), 0)
                            FROM fact_player_count pc
                            JOIN game_developers gd ON pc.game_id = gd.game_id
                            WHERE gd.developer_id = %s
                            """, (developer_id,))
            stats['total_current_players'] = cursor.fetchone()[0] or 0
        except:
            stats['total_current_players'] = 0
        
        try:
            cursor.execute("""
                            SELECT COALESCE(AVG(r.score), 0)
                            FROM fact_reviews r
                            JOIN game_developers gd ON r.game_id = gd.game_id
                            WHERE gd.developer_id = %s
                            """, (developer_id,))
            stats['avg_score'] = cursor.fetchone()[0] or 0
        except:
            stats['avg_score'] = 0
        
        try:
            cursor.execute("""
                            SELECT COALESCE(MAX(pc.peak_players), 0)
                            FROM fact_player_count pc
                            JOIN game_developers gd ON pc.game_id = gd.game_id
                            WHERE gd.developer_id = %s
                            """, (developer_id,))
            stats['highest_peak'] = cursor.fetchone()[0] or 0
        except:
            stats['highest_peak'] = 0
        
        return stats
    
    def load_game_info(self, game_id):
        info = {}
        
        cursor.execute("""
                        SELECT title, release_date, game_desc
                        FROM dim_game
                        WHERE game_id = %s
                        """, (game_id,))
        result = cursor.fetchone()
        if result:
            info['title'] = result[0]
            info['release_date'] = result[1]
            info['description'] = result[2] or "No description available"
        
        cursor.execute("""
                        SELECT g.genre
                        FROM dim_genre g
                        JOIN game_genre gg ON g.genre_id = gg.genre_id
                        WHERE gg.game_id = %s
                        ORDER BY g.genre
                        """, (game_id,))
        info['genres'] = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("""
                        SELECT p.name
                        FROM dim_publisher p
                        JOIN game_publisher gp ON p.publisher_id = gp.publisher_id
                        WHERE gp.game_id = %s
                        ORDER BY p.name
                        """, (game_id,))
        info['publishers'] = [row[0] for row in cursor.fetchall()]
        
        return info
    
    # === Chart Update Methods ===
    
    def update_player_chart(self, game_id):
        dates, peak_players, avg_players = self.load_player_count_data(game_id)
        
        if not dates:
            return
        
        self.player_chart.clear()
        
        date_axis = pg.DateAxisItem(orientation='bottom')
        self.player_chart.setAxisItems({'bottom': date_axis})
        
        clean_dates = []
        clean_peaks = []
        clean_avgs = []
        
        for d, p, a in zip(dates, peak_players, avg_players):
            if d is not None and p is not None and a is not None:
                try:
                    if isinstance(d, dt):
                        clean_dates.append(d.timestamp())
                    else:
                        clean_dates.append(dt.strptime(str(d), '%Y-%m-%d').timestamp())
                    clean_peaks.append(float(p))
                    clean_avgs.append(float(a))
                except (ValueError, TypeError):
                    continue
        
        if not clean_dates:
            return
        
        peak_pen = pg.mkPen(color='#0078d4', width=2)
        self.player_chart.plot(clean_dates, clean_peaks, pen=peak_pen, name='Peak Players')
        
        avg_pen = pg.mkPen(color='#ff8c00', width=2, style=pg.QtCore.Qt.DashLine)
        self.player_chart.plot(clean_dates, clean_avgs, pen=avg_pen, name='Avg Players')
    
    def update_review_chart(self, game_id):
        scores, total_reviews, sentiment, dates = self.load_review_data(game_id)
        
        if not dates:
            return
        
        self.review_chart.clear()
        
        date_axis = pg.DateAxisItem(orientation='bottom')
        self.review_chart.setAxisItems({'bottom': date_axis})
        
        clean_dates = []
        clean_scores = []
        
        for d, s in zip(dates, scores):
            if d is not None and s is not None:
                try:
                    if isinstance(d, dt):
                        clean_dates.append(d.timestamp())
                    else:
                        clean_dates.append(dt.strptime(str(d), '%Y-%m-%d').timestamp())
                    clean_scores.append(float(s))
                except (ValueError, TypeError):
                    continue
        
        if not clean_dates:
            return
        
        score_pen = pg.mkPen(color='#28a745', width=2)
        self.review_chart.plot(clean_dates, clean_scores, pen=score_pen, name='Review Score')
        self.review_chart.setLabel('left', 'Score (0-100)')
    
    def update_patch_chart(self, game_id):
        dates, peak_players, _ = self.load_player_count_data(game_id)
        versions, patch_types, patch_dates = self.load_patch_data(game_id)
        
        self.patch_chart.clear()
        
        if not dates:
            return
        
        date_axis = pg.DateAxisItem(orientation='bottom')
        self.patch_chart.setAxisItems({'bottom': date_axis})
        
        clean_dates = []
        clean_peaks = []
        
        for d, p in zip(dates, peak_players):
            if d is not None and p is not None:
                try:
                    if isinstance(d, dt):
                        clean_dates.append(d.timestamp())
                    else:
                        clean_dates.append(dt.strptime(str(d), '%Y-%m-%d').timestamp())
                    clean_peaks.append(float(p))
                except (ValueError, TypeError):
                    continue
        
        if clean_dates:
            player_pen = pg.mkPen(color='#0078d4', width=1, alpha=128)
            self.patch_chart.plot(clean_dates, clean_peaks, pen=player_pen, name='Peak Players')
        
        if patch_dates:
            patch_text = "Patches:\n"
            for ver, ptype, pdate in zip(versions, patch_types, patch_dates):
                if pdate is None:
                    continue
                try:
                    if isinstance(pdate, dt):
                        patch_ts = pdate.timestamp()
                    else:
                        patch_ts = dt.strptime(str(pdate), '%Y-%m-%d').timestamp()
                    color = '#dc3545' if ptype and 'major' in str(ptype).lower() else '#ffc107'
                    vline = pg.InfiniteLine(pos=patch_ts, angle=90, pen=pg.mkPen(color=color, width=2, alpha=180))
                    self.patch_chart.addItem(vline)
                    patch_text += f"• {pdate}: {ver} ({ptype})\n"
                except (ValueError, TypeError):
                    continue
            
            self.patch_list_label.setText(patch_text)
    
    def update_platform_chart(self, game_id):
        platforms = self.load_platform_data(game_id)
        
        self.platform_chart.clear()
        
        if not platforms:
            return
        
        counts = Counter(platforms)
        sorted_items = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        labels = [item[0] for item in sorted_items]
        heights = [item[1] for item in sorted_items]
        
        x = list(range(len(labels)))
        colors = [pg.mkColor(0, 120, 212, 200 - (i * 30)) for i in range(len(labels))]
        
        bargraph = pg.BarGraphItem(x=x, height=heights, width=0.6, brushes=colors)
        self.platform_chart.addItem(bargraph)
        
        for i, (label, height) in enumerate(zip(labels, heights)):
            text = pg.TextItem(str(height), color='#ffffff', anchor=(0.5, -0.5))
            text.setPos(i, height)
            self.platform_chart.addItem(text)
            
            label_text = pg.TextItem(label, color='#ffffff', anchor=(0, 1))
            label_text.setPos(i, -0.1)
            label_text.setAngle(-45)
            self.platform_chart.addItem(label_text)
    
    def update_developer_stats(self, developer_id):
        stats = self.load_developer_stats(developer_id)
        
        self.total_games_label.setText(f"📊 Total Games: {stats['total_games']}")
        self.total_players_label.setText(f"👥 Current Total Players: {stats['total_current_players']:,.0f}")
        self.avg_score_label.setText(f"⭐ Average Review Score: {stats['avg_score']:.1f}")
        self.highest_peak_label.setText(f"🏆 All-Time Highest Peak: {stats['highest_peak']:,.0f}")
    
    def update_info_tab(self, game_id):
        info = self.load_game_info(game_id)
        
        self.game_title_label.setText(info.get('title', 'Unknown Game'))
        
        release_date = info.get('release_date')
        if release_date:
            self.release_date_label.setText(f"📅 Release Date: {release_date}")
        else:
            self.release_date_label.setText("📅 Release Date: Unknown")
        
        self.desc_label.setText(info.get('description', 'No description available'))
        
        genres = info.get('genres', [])
        if genres:
            self.genres_label.setText("• " + "\n• ".join(genres))
        else:
            self.genres_label.setText("No genres listed")
        
        publishers = info.get('publishers', [])
        if publishers:
            self.publishers_label.setText("• " + "\n• ".join(publishers))
        else:
            self.publishers_label.setText("No publishers listed")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    window = GameAnalyticsDashboard()
    window.show()
    sys.exit(app.exec())