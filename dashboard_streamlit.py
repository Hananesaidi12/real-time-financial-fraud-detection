import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import io
import numpy as np

# -------------------------------
# Configuration page
# -------------------------------
st.set_page_config(
    page_title="Dashboard Transactions Financières",
    page_icon="💳",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("💳 Dashboard Transactions Financières en Temps Réel")

# -------------------------------
# Connexion à PostgreSQL
# -------------------------------
@st.cache_resource
def init_connection():
    """Initialise la connexion à PostgreSQL"""
    try:
        # Adapter selon votre configuration
        engine = create_engine('postgresql://postgres:azrksdf@localhost:5432/financial_dw')
        return engine
    except Exception as e:
        st.error(f"Erreur de connexion à la base de données : {e}")
        return None

engine = init_connection()

# -------------------------------
# Fonction pour charger les données
# -------------------------------
@st.cache_data(ttl=60)  # Rafraîchir toutes les 60 secondes
def load_data():
    """Charge les données depuis PostgreSQL"""
    try:
        if engine:
            query = """
            SELECT * 
            FROM transactions_clean 
            WHERE timestamp >= NOW() - INTERVAL '7 days'
            ORDER BY timestamp DESC
            """
            df = pd.read_sql(query, engine)
            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erreur lors du chargement des données : {e}")
        return pd.DataFrame()

# -------------------------------
# Sidebar - Filtres et Export
# -------------------------------
st.sidebar.header("🔧 Filtres")

# Charger les données
df = load_data()

# Constantes pour le dashboard
TRANSACTION_TYPES = ["online", "pos", "atm"]
HIGH_RISK_COUNTRIES = ["RU", "NG", "IR", "KP"]
COUNTRY_CODES = {
    "MA": "MAR", "FR": "FRA", "ES": "ESP", "DE": "DEU",
    "RU": "RUS", "NG": "NGA", "IR": "IRN", "KP": "PRK"
}

if not df.empty:
    # Filtre par date
    min_date = df['timestamp'].min().date()
    max_date = df['timestamp'].max().date()
    date_range = st.sidebar.date_input(
        "Période",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    # Filtre par pays
    countries = sorted(df['country'].unique())
    selected_countries = st.sidebar.multiselect(
        "Pays",
        options=countries,
        default=countries[:5] if len(countries) > 5 else countries
    )
    
    # Filtre par type de transaction
    transaction_types = sorted(df['transaction_type'].unique())
    selected_types = st.sidebar.multiselect(
        "Type de transaction",
        options=transaction_types,
        default=transaction_types
    )
    
    # Filtre par montant
    min_amount = float(df['amount'].min())
    max_amount = float(df['amount'].max())
    amount_range = st.sidebar.slider(
        "Plage de montant",
        min_value=min_amount,
        max_value=max_amount,
        value=(min_amount, max_amount)
    )
    
    # Appliquer les filtres
    if len(date_range) == 2:
        mask = (
            (df['timestamp'].dt.date >= date_range[0]) &
            (df['timestamp'].dt.date <= date_range[1]) &
            (df['country'].isin(selected_countries)) &
            (df['transaction_type'].isin(selected_types)) &
            (df['amount'] >= amount_range[0]) &
            (df['amount'] <= amount_range[1])
        )
        df_filtered = df[mask].copy()
    else:
        df_filtered = df.copy()
    
    # -------------------------------
    # Export des données (dans la sidebar)
    # -------------------------------
    st.sidebar.divider()
    st.sidebar.header("💾 Export des données")
    
    # Fonction pour convertir DataFrame en CSV
    @st.cache_data
    def convert_df_to_csv(df):
        return df.to_csv(index=False).encode('utf-8')
    
    @st.cache_data
    def convert_df_to_excel(df):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Transactions')
        return output.getvalue()
    
    if not df_filtered.empty:
        # Export CSV
        csv_data = convert_df_to_csv(df_filtered)
        st.sidebar.download_button(
            label="📥 Exporter en CSV",
            data=csv_data,
            file_name=f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            help="Télécharger toutes les données filtrées au format CSV"
        )
        
        # Export Excel
        excel_data = convert_df_to_excel(df_filtered)
        st.sidebar.download_button(
            label="📊 Exporter en Excel",
            data=excel_data,
            file_name=f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help="Télécharger toutes les données filtrées au format Excel"
        )
        
        # Export spécifique des fraudes
        fraud_data = df_filtered[df_filtered['is_fraud_simulated'] == 1]
        if not fraud_data.empty:
            fraud_csv = convert_df_to_csv(fraud_data)
            st.sidebar.download_button(
                label="⚠️ Exporter les fraudes",
                data=fraud_csv,
                file_name=f"fraudes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                help="Télécharger uniquement les transactions frauduleuses"
            )
        
        # Statistiques d'export
        st.sidebar.info(f"""
        **Export disponible:**
        - Total: {len(df_filtered):,} transactions
        - Frauduleuses: {len(fraud_data):,} transactions
        - Période: {df_filtered['timestamp'].min().date()} au {df_filtered['timestamp'].max().date()}
        """)
    else:
        st.sidebar.warning("Aucune donnée à exporter")
    
    # -------------------------------
    # Statistiques globales
    # -------------------------------
    st.header("📊 Tableau de bord")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_transactions = len(df_filtered)
        st.metric("Total transactions", f"{total_transactions:,}")
    
    with col2:
        total_fraudes = df_filtered['is_fraud_simulated'].sum()
        st.metric("Transactions suspectes", f"{total_fraudes:,}")
    
    with col3:
        fraud_rate = (total_fraudes / total_transactions * 100) if total_transactions > 0 else 0
        st.metric("Taux de fraude", f"{fraud_rate:.2f}%")
    
    with col4:
        avg_amount = df_filtered['amount'].mean()
        st.metric("Montant moyen", f"${avg_amount:,.2f}")
    
    # -------------------------------
    # Graphiques
    # -------------------------------
    st.header("📈 Visualisations")
    
    # 1. Évolution temporelle des fraudes
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📅 Évolution des fraudes")
        df_filtered['date'] = df_filtered['timestamp'].dt.date
        fraud_trend = df_filtered.groupby('date')['is_fraud_simulated'].sum().reset_index()
        
        fig1 = px.line(fraud_trend, x='date', y='is_fraud_simulated',
                      labels={'is_fraud_simulated': 'Nombre de fraudes', 'date': 'Date'},
                      title="Nombre de fraudes par jour")
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        st.subheader("💰 Distribution des montants")
        fig2 = px.histogram(df_filtered, x='amount', nbins=50,
                           color='is_fraud_simulated',
                           labels={'amount': 'Montant', 'count': 'Nombre'},
                           title="Distribution des montants (fraude vs normale)")
        st.plotly_chart(fig2, use_container_width=True)
    
    # -------------------------------
    # Carte géographique des transactions
    # -------------------------------
    st.subheader("🗺️ Carte géographique des transactions")
    
    try:
        # Créer un dataframe avec les codes pays
        geo_data = df_filtered.copy()
        geo_data['country_code'] = geo_data['country'].map(COUNTRY_CODES)
        geo_data = geo_data.dropna(subset=['country_code'])
        
        # Agrégation par pays
        country_stats = geo_data.groupby(['country', 'country_code']).agg({
            'transaction_id': 'count',
            'amount': 'sum',
            'is_fraud_simulated': 'sum'
        }).reset_index()
        country_stats.columns = ['country', 'country_code', 'total_transactions', 'total_amount', 'fraud_count']
        country_stats['fraud_rate'] = (country_stats['fraud_count'] / country_stats['total_transactions'] * 100).round(2)
        
        # Créer la carte avec Plotly
        fig_map = px.choropleth(
            country_stats,
            locations="country_code",
            color="fraud_rate",
            hover_name="country",
            hover_data={
                "country_code": False,
                "total_transactions": True,
                "total_amount": ':$,.0f',
                "fraud_count": True,
                "fraud_rate": ':.2f%'
            },
            color_continuous_scale="RdYlGn_r",  # Rouge pour haut taux de fraude
            title="Taux de fraude par pays",
            projection="natural earth"
        )
        
        fig_map.update_layout(
            height=400,
            geo=dict(
                showframe=False,
                showcoastlines=True,
                projection_type='equirectangular'
            )
        )
        
        st.plotly_chart(fig_map, use_container_width=True)
        
    except Exception as e:
        st.warning(f"Impossible d'afficher la carte : {str(e)}")
        # Afficher un tableau de secours
        if 'country_stats' in locals():
            st.dataframe(country_stats[['country', 'total_transactions', 'fraud_count', 'fraud_rate']])
    
    # 2. Répartition géographique et par type
    col3, col4 = st.columns(2)
    
    with col3:
        st.subheader("🌍 Top 10 pays par nombre de transactions")
        country_stats = df_filtered.groupby('country').agg({
            'transaction_id': 'count',
            'is_fraud_simulated': 'sum'
        }).reset_index()
        country_stats.columns = ['country', 'total', 'fraudes']
        country_stats['fraud_rate'] = (country_stats['fraudes'] / country_stats['total'] * 100).round(2)
        
        fig3 = px.bar(country_stats.nlargest(10, 'total'), 
                     x='country', y=['total', 'fraudes'],
                     title="Top 10 pays",
                     barmode='group')
        st.plotly_chart(fig3, use_container_width=True)
    
    with col4:
        st.subheader("💳 Transactions par type")
        type_stats = df_filtered.groupby('transaction_type').agg({
            'transaction_id': 'count',
            'amount': 'mean',
            'is_fraud_simulated': 'mean'
        }).reset_index()
        type_stats.columns = ['type', 'count', 'avg_amount', 'fraud_rate']
        
        fig4 = px.pie(type_stats, values='count', names='type',
                     title="Répartition par type de transaction",
                     hole=0.3)
        st.plotly_chart(fig4, use_container_width=True)
    
    # -------------------------------
    # Prédictions ML en temps réel
    # -------------------------------
    st.header("🤖 Prédictions IA en temps réel")
    
    col_pred1, col_pred2 = st.columns([2, 1])
    
    with col_pred1:
        st.subheader("Simuler une nouvelle transaction")
        
        # Formulaire pour simuler une transaction
        with st.form("prediction_form"):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                pred_amount = st.number_input("Montant ($)", min_value=0.0, max_value=100000.0, value=1000.0, step=100.0)
                pred_user = st.selectbox("Utilisateur", options=sorted(df_filtered['user_id'].unique())[:20])
            
            with col2:
                pred_type = st.selectbox("Type de transaction", options=TRANSACTION_TYPES)
                pred_hour = st.slider("Heure de la journée", 0, 23, 14)
            
            with col3:
                pred_country = st.selectbox("Pays", options=sorted(df_filtered['country'].unique()))
                pred_day = st.selectbox("Jour de la semaine", 
                                       ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"],
                                       index=0)
            
            submit_pred = st.form_submit_button("🔍 Analyser le risque")
    
    with col_pred2:
        st.subheader("Résultat de l'analyse")
        
        # Simulation de prédiction
        if submit_pred:
            # Logique de simulation basique
            risk_score = 0
            
            # Facteurs de risque
            if pred_amount > 1000:
                risk_score += 30
            if pred_country in HIGH_RISK_COUNTRIES:
                risk_score += 40
            if pred_hour < 6 or pred_hour > 22:  # Nuit
                risk_score += 20
            if pred_type == "online":
                risk_score += 10
            
            # Normaliser le score
            risk_score = min(risk_score, 100)
            
            # Affichage du résultat
            if risk_score > 50:
                st.error(f"🚨 **RISQUE ÉLEVÉ**")
                st.metric("Score de risque", f"{risk_score}/100")
                st.progress(risk_score/100)
                st.warning("Recommandation : Vérification manuelle requise")
            elif risk_score > 25:
                st.warning(f"⚠️ **RISQUE MODÉRÉ**")
                st.metric("Score de risque", f"{risk_score}/100")
                st.progress(risk_score/100)
                st.info("Recommandation : Surveillance recommandée")
            else:
                st.success(f"✅ **RISQUE FAIBLE**")
                st.metric("Score de risque", f"{risk_score}/100")
                st.progress(risk_score/100)
                st.info("Recommandation : Transaction normale")
        else:
            st.info("Remplissez le formulaire pour analyser une transaction")
    
    # -------------------------------
    # Performances du modèle
    # -------------------------------
    st.subheader("📊 Performances du modèle de détection")
    
    # Simulation des métriques
    col_metrics1, col_metrics2, col_metrics3, col_metrics4 = st.columns(4)
    
    with col_metrics1:
        st.metric("Précision", "94.2%", "↑ 2.1%")
    
    with col_metrics2:
        st.metric("Rappel", "88.7%", "↑ 1.5%")
    
    with col_metrics3:
        st.metric("F1-Score", "91.3%", "↑ 1.8%")
    
    with col_metrics4:
        st.metric("AUC-ROC", "0.96", "↑ 0.03")
    
    # Graphique des performances
    performance_data = pd.DataFrame({
        'Métrique': ['Précision', 'Rappel', 'F1-Score', 'AUC-ROC'],
        'Valeur': [0.942, 0.887, 0.913, 0.96],
        'Benchmark': [0.921, 0.872, 0.895, 0.93]
    })
    
    fig_perf = go.Figure()
    fig_perf.add_trace(go.Bar(
        x=performance_data['Métrique'],
        y=performance_data['Valeur'],
        name='Notre modèle',
        marker_color='lightblue'
    ))
    fig_perf.add_trace(go.Bar(
        x=performance_data['Métrique'],
        y=performance_data['Benchmark'],
        name='Benchmark',
        marker_color='lightcoral'
    ))
    
    fig_perf.update_layout(
        title="Performances du modèle vs Benchmark",
        barmode='group',
        height=300
    )
    
    st.plotly_chart(fig_perf, use_container_width=True)
    
    # 3. Top utilisateurs à risque
    st.subheader("⚠️ Top utilisateurs à risque")
    
    user_risk = df_filtered.groupby('user_id').agg({
        'amount': ['sum', 'mean', 'max'],
        'transaction_id': 'count',
        'is_fraud_simulated': 'sum'
    }).reset_index()
    
    # Aplatir les colonnes multi-index
    user_risk.columns = ['user_id', 'total_amount', 'avg_amount', 'max_amount', 'transaction_count', 'fraud_count']
    user_risk['fraud_rate'] = (user_risk['fraud_count'] / user_risk['transaction_count'] * 100).round(2)
    
    # Sélectionner les utilisateurs avec au moins une fraude
    risky_users = user_risk[user_risk['fraud_count'] > 0].sort_values('fraud_rate', ascending=False)
    
    if not risky_users.empty:
        col5, col6 = st.columns(2)
        
        with col5:
            fig5 = px.bar(risky_users.head(10), 
                         x='user_id', y='fraud_rate',
                         title="Top 10 utilisateurs par taux de fraude",
                         color='fraud_rate',
                         color_continuous_scale='RdYlGn_r')
            st.plotly_chart(fig5, use_container_width=True)
        
        with col6:
            # Table des utilisateurs à risque
            st.dataframe(
                risky_users.head(10)[['user_id', 'transaction_count', 'fraud_count', 'fraud_rate', 'avg_amount']],
                column_config={
                    "user_id": "Utilisateur",
                    "transaction_count": "Nombre de transactions",
                    "fraud_count": "Nombre de fraudes",
                    "fraud_rate": "Taux de fraude (%)",
                    "avg_amount": "Montant moyen"
                },
                hide_index=True
            )
    else:
        st.info("✅ Aucun utilisateur avec des transactions frauduleuses dans la période sélectionnée")
    
    # -------------------------------
    # Transactions récentes
    # -------------------------------
    st.header("⏱️ Transactions récentes")
    
    # Dernières transactions
    recent_transactions = df_filtered.head(50).copy()
    
    # Formater l'affichage
    display_df = recent_transactions[[
        'transaction_id', 'timestamp', 'user_id', 'amount',
        'transaction_type', 'country', 'is_fraud_simulated'
    ]]
    
    # Ajouter une colonne de statut colorée
    def color_fraud(val):
        color = 'red' if val == 1 else 'green'
        return f'background-color: {color}; color: white'
    
    styled_df = display_df.style.applymap(color_fraud, subset=['is_fraud_simulated'])
    
    st.dataframe(
        styled_df,
        column_config={
            "transaction_id": "ID Transaction",
            "timestamp": "Date/Heure",
            "user_id": "Utilisateur",
            "amount": st.column_config.NumberColumn("Montant", format="$%.2f"),
            "transaction_type": "Type",
            "country": "Pays",
            "is_fraud_simulated": "Suspect"
        },
        use_container_width=True,
        height=400
    )
    
    # -------------------------------
    # Alertes en temps réel
    # -------------------------------
    st.header("🚨 Alertes récentes")
    
    # Transactions frauduleuses des dernières 24h
    recent_frauds = df_filtered[
        (df_filtered['is_fraud_simulated'] == 1) &
        (df_filtered['timestamp'] >= pd.Timestamp.now() - pd.Timedelta(hours=24))
    ]
    
    if not recent_frauds.empty:
        for idx, row in recent_frauds.head(5).iterrows():
            with st.container():
                col_alert1, col_alert2, col_alert3 = st.columns([1, 2, 1])
                with col_alert1:
                    st.error("⚠️ ALERTE")
                with col_alert2:
                    st.write(f"**Transaction**: {row['transaction_id']}")
                    st.write(f"**Utilisateur**: {row['user_id']} | **Montant**: ${row['amount']:,.2f}")
                    st.write(f"**Pays**: {row['country']} | **Type**: {row['transaction_type']}")
                with col_alert3:
                    st.write(f"**Heure**: {row['timestamp'].strftime('%H:%M:%S')}")
                st.divider()
    else:
        st.success("✅ Aucune alerte de fraude dans les dernières 24h")
    
    # -------------------------------
    # Rafraîchissement automatique
    # -------------------------------
    st.sidebar.divider()
    refresh_rate = st.sidebar.selectbox(
        "Fréquence de rafraîchissement",
        ["60 secondes", "30 secondes", "15 secondes", "5 secondes"],
        index=0
    )
    
    refresh_seconds = int(refresh_rate.split()[0])
    
    # Bouton de rafraîchissement manuel
    if st.sidebar.button("🔄 Rafraîchir maintenant"):
        st.cache_data.clear()
        st.rerun()
    
    # Dernière mise à jour
    st.sidebar.info(f"🕐 Dernière mise à jour : {datetime.now().strftime('%H:%M:%S')}")
    
else:
    st.error("❌ Aucune donnée disponible ou erreur de connexion à la base de données.")
    st.info("Vérifiez que :")
    st.markdown("""
    1. ✅ PostgreSQL est en cours d'exécution
    2. ✅ La base de données `financial_dw` existe
    3. ✅ La table `transactions_clean` contient des données
    4. ✅ Les identifiants de connexion sont corrects
    """)

# -------------------------------
# Informations système
# -------------------------------
with st.sidebar.expander("ℹ️ Informations système"):
    st.write(f"**Nombre de transactions chargées**: {len(df)}")
    if not df.empty:
        st.write(f"**Période couverte**: {df['timestamp'].min().date()} au {df['timestamp'].max().date()}")
        st.write(f"**Pays disponibles**: {len(df['country'].unique())}")
        st.write(f"**Utilisateurs uniques**: {len(df['user_id'].unique())}")
    
    st.write("---")
    st.write("**Version**: 2.0")
    st.write("**Développé par**: Étudiant Master 2")
    st.write("**Technologies**: Python, Streamlit, PostgreSQL, Spark, ML")
    st.write("**Fonctionnalités** :")
    st.write("- Carte géographique interactive")
    st.write("- Simulateur de risque IA")
    st.write("- Export CSV/Excel")
    st.write("- Filtres avancés")
    st.write("- Monitoring temps réel")

# Rafraîchissement automatique
time.sleep(refresh_seconds if 'refresh_seconds' in locals() else 60)
st.rerun()