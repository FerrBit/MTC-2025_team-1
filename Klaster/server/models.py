import json
import uuid
import numpy as np
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from datetime import datetime, timezone

db = SQLAlchemy()
bcrypt = Bcrypt()

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    clustering_sessions = db.relationship('ClusteringSession', backref='user', lazy=True)

    def set_password(self, password):
        self.password_hash = bcrypt.generate_password_hash(password).decode('utf-8')

    def check_password(self, password):
        return bcrypt.check_password_hash(self.password_hash, password)

    def __repr__(self):
        return f'<User {self.username}>'

class ClusteringSession(db.Model):
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    status = db.Column(db.String(50), nullable=False, default='STARTED')
    algorithm = db.Column(db.String(50), nullable=True)
    params_json = db.Column(db.Text, nullable=True)
    input_file_path = db.Column(db.String(512), nullable=True)
    original_input_filename = db.Column(db.String(255), nullable=True)
    image_archive_path = db.Column(db.String(512), nullable=True)
    original_archive_filename = db.Column(db.String(255), nullable=True)
    result_message = db.Column(db.Text, nullable=True)
    num_clusters = db.Column(db.Integer, nullable=True)
    processing_time_sec = db.Column(db.Float, nullable=True)
    scatter_data_file_path = db.Column(db.String(512), nullable=True)

    clusters = db.relationship('ClusterMetadata', backref='session', lazy='dynamic', cascade="all, delete-orphan")
    adjustments = db.relationship('ManualAdjustmentLog', backref='session', lazy='dynamic', cascade="all, delete-orphan")

    def set_params(self, params_dict):
        self.params_json = json.dumps(params_dict)

    def get_params(self):
        try:
            return json.loads(self.params_json) if self.params_json else {}
        except json.JSONDecodeError:
            return {}

    def __repr__(self):
        return f'<ClusteringSession {self.id} [{self.status}]>'

class ClusterMetadata(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    session_id = db.Column(db.String(36), db.ForeignKey('clustering_session.id', ondelete='CASCADE'), nullable=False)
    cluster_label = db.Column(db.String(50), nullable=False)
    original_cluster_id = db.Column(db.String(50), nullable=True)
    centroid_json = db.Column(db.Text, nullable=True)
    centroid_2d_json = db.Column(db.Text, nullable=True)
    size = db.Column(db.Integer, nullable=True)
    contact_sheet_path = db.Column(db.String(512), nullable=True)
    metrics_json = db.Column(db.Text, nullable=True)
    is_deleted = db.Column(db.Boolean, default=False, nullable=False)
    name = db.Column(db.String(100), nullable=True)

    def set_centroid(self, centroid_vector):
        if isinstance(centroid_vector, np.ndarray):
            self.centroid_json = json.dumps(centroid_vector.tolist())
        elif isinstance(centroid_vector, (list, tuple)):
             self.centroid_json = json.dumps(centroid_vector)
        else:
            self.centroid_json = None

    def get_centroid(self):
        try:
            return np.array(json.loads(self.centroid_json)) if self.centroid_json else None
        except json.JSONDecodeError:
            return None

    def set_centroid_2d(self, centroid_2d_coords):
         if isinstance(centroid_2d_coords, (np.ndarray, list, tuple)) and len(centroid_2d_coords) == 2:
             self.centroid_2d_json = json.dumps([float(c) for c in centroid_2d_coords])
         else:
             self.centroid_2d_json = None

    def get_centroid_2d(self):
        try:
            return json.loads(self.centroid_2d_json) if self.centroid_2d_json else None
        except json.JSONDecodeError:
            return None

    def set_metrics(self, metrics_dict):
        self.metrics_json = json.dumps(metrics_dict)

    def get_metrics(self):
        try:
            return json.loads(self.metrics_json) if self.metrics_json else {}
        except json.JSONDecodeError:
            return {}

    def __repr__(self):
        return f'<ClusterMetadata Session={self.session_id} Label={self.cluster_label} Size={self.size}>'

class ManualAdjustmentLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    session_id = db.Column(db.String(36), db.ForeignKey('clustering_session.id', ondelete='CASCADE'), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    timestamp = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    action_type = db.Column(db.String(50), nullable=False)
    details_json = db.Column(db.Text, nullable=True)

    user = db.relationship('User')

    def set_details(self, details_dict):
        self.details_json = json.dumps(details_dict)

    def get_details(self):
        try:
            return json.loads(self.details_json) if self.details_json else {}
        except json.JSONDecodeError:
            return {}

    def __repr__(self):
        return f'<ManualAdjustmentLog Session={self.session_id} Action={self.action_type}>'