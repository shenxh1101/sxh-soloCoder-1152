import json
import time
import copy
import uuid
import os
import threading
from collections import defaultdict

from flask import Flask, render_template, request, send_file, jsonify
from flask_socketio import SocketIO, emit, join_room, leave_room, disconnect

app = Flask(__name__)
app.config['SECRET_KEY'] = 'ot-demo-secret-key-2024'
socketio = SocketIO(app, cors_allowed_origins='*', ping_timeout=10, ping_interval=5,
                    async_mode='threading')

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
DOCUMENTS = {}
HEARTBEAT_TIMEOUT = 15
HEARTBEAT_CHECK_INTERVAL = 5

DEFAULT_DOC = {
    "product_catalog": {
        "name": "电子产品目录",
        "version": "1.0.0",
        "categories": {
            "computers": {
                "name": "电脑",
                "products": {
                    "laptop_pro": {
                        "name": "专业笔记本电脑",
                        "price": 8999,
                        "specs": {"cpu": "i7-13700H", "ram": "16GB", "storage": "512GB SSD"},
                        "in_stock": True
                    },
                    "desktop_elite": {
                        "name": "精英台式机",
                        "price": 12999,
                        "specs": {"cpu": "i9-13900K", "ram": "32GB", "storage": "1TB SSD"},
                        "in_stock": False
                    }
                }
            },
            "phones": {
                "name": "手机",
                "products": {
                    "phone_x": {
                        "name": "Phone X",
                        "price": 6999,
                        "specs": {"screen": "6.7英寸", "battery": "4500mAh", "storage": "256GB"},
                        "in_stock": True
                    }
                }
            }
        },
        "settings": {
            "currency": "CNY",
            "tax_rate": 0.13,
            "free_shipping_threshold": 99
        }
    }
}


def _get_value_at(obj, path):
    current = obj
    for key in path:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    return current


def _set_value_inplace(obj, path, value):
    if not path:
        obj.clear()
        obj.update(value) if isinstance(value, dict) else None
        return
    current = obj
    for key in path[:-1]:
        if key not in current or not isinstance(current[key], dict):
            current[key] = {}
        current = current[key]
    current[path[-1]] = value


def _add_node_inplace(obj, path, key, value):
    current = obj
    for k in path:
        if k not in current or not isinstance(current[k], dict):
            current[k] = {}
        current = current[k]
    if key not in current:
        current[key] = value


def _delete_path_inplace(obj, path):
    current = obj
    for key in path[:-1]:
        if key not in current:
            return
        current = current[key]
    if path[-1] in current:
        del current[path[-1]]


def compute_diff(obj1, obj2, path=None):
    if path is None:
        path = []
    diffs = []

    if type(obj1) != type(obj2):
        diffs.append({
            'path': list(path), 'type': 'modified',
            'from_value': _serializable(obj1), 'to_value': _serializable(obj2)
        })
        return diffs

    if isinstance(obj1, dict):
        all_keys = set(obj1.keys()) | set(obj2.keys())
        for key in sorted(all_keys):
            child_path = list(path) + [key]
            if key in obj1 and key not in obj2:
                diffs.append({'path': child_path, 'type': 'removed', 'from_value': _serializable(obj1[key])})
            elif key not in obj1 and key in obj2:
                diffs.append({'path': child_path, 'type': 'added', 'to_value': _serializable(obj2[key])})
            elif obj1[key] != obj2[key]:
                if isinstance(obj1[key], (dict, list)) and isinstance(obj2[key], (dict, list)):
                    diffs.extend(compute_diff(obj1[key], obj2[key], child_path))
                else:
                    diffs.append({
                        'path': child_path, 'type': 'modified',
                        'from_value': _serializable(obj1[key]),
                        'to_value': _serializable(obj2[key])
                    })

    elif isinstance(obj1, list):
        max_len = max(len(obj1), len(obj2))
        for i in range(max_len):
            child_path = list(path) + [str(i)]
            if i >= len(obj1):
                diffs.append({'path': child_path, 'type': 'added', 'to_value': _serializable(obj2[i])})
            elif i >= len(obj2):
                diffs.append({'path': child_path, 'type': 'removed', 'from_value': _serializable(obj1[i])})
            elif obj1[i] != obj2[i]:
                if isinstance(obj1[i], (dict, list)) and isinstance(obj2[i], (dict, list)):
                    diffs.extend(compute_diff(obj1[i], obj2[i], child_path))
                else:
                    diffs.append({
                        'path': child_path, 'type': 'modified',
                        'from_value': _serializable(obj1[i]),
                        'to_value': _serializable(obj2[i])
                    })
    else:
        if obj1 != obj2:
            diffs.append({
                'path': list(path), 'type': 'modified',
                'from_value': _serializable(obj1), 'to_value': _serializable(obj2)
            })

    return diffs


def _serializable(val):
    if val is None or isinstance(val, (bool, int, float, str)):
        return val
    try:
        return json.dumps(val, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(val)


class Document:
    def __init__(self, doc_id, initial_data=None):
        self.doc_id = doc_id
        self.data = copy.deepcopy(initial_data) if initial_data else copy.deepcopy(DEFAULT_DOC)
        self.version = 0
        self.operations = []
        self.snapshots = {0: copy.deepcopy(self.data)}
        self.clients = {}
        self.last_heartbeat = {}

    def get_value(self, path):
        current = self.data
        for key in path:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current

    def set_value(self, path, value):
        if not path:
            self.data = value
            return
        current = self.data
        for key in path[:-1]:
            if key not in current or not isinstance(current[key], dict):
                current[key] = {}
            current = current[key]
        current[path[-1]] = value

    def delete_path(self, path):
        current = self.data
        for key in path[:-1]:
            if key not in current:
                return False
            current = current[key]
        if path[-1] in current:
            del current[path[-1]]
            return True
        return False

    def add_node(self, path, key, value):
        current = self.data
        for k in path:
            if k not in current or not isinstance(current[k], dict):
                current[k] = {}
            current = current[k]
        if key not in current:
            current[key] = value
            return True
        return False

    def move_node(self, from_path, to_path):
        value = self.get_value(from_path)
        if value is None:
            return False
        self.delete_path(from_path)
        current = self.data
        for key in to_path[:-1]:
            if key not in current or not isinstance(current[key], dict):
                current[key] = {}
            current = current[key]
        current[to_path[-1]] = value
        return True

    def apply_operation(self, op):
        op_type = op['type']
        if op_type in ('update', 'replace'):
            self.set_value(op['path'], op['value'])
        elif op_type == 'add':
            self.add_node(op['path'], op['key'], op['value'])
        elif op_type == 'delete':
            self.delete_path(op['path'])
        elif op_type == 'move':
            self.move_node(op['from_path'], op['to_path'])
        self.version += 1
        op['applied_version'] = self.version
        self.operations.append(op)
        if self.version % 5 == 0 or self.version < 5:
            self.snapshots[self.version] = copy.deepcopy(self.data)
        self._save_to_disk()

    def _save_to_disk(self):
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            filepath = os.path.join(DATA_DIR, f'{self.doc_id}.json')
            payload = {
                'data': self.data,
                'version': self.version,
                'operations': self.operations,
                'snapshots': {str(k): v for k, v in self.snapshots.items()}
            }
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f'[PERSIST] Failed to save doc {self.doc_id}: {e}')

    @staticmethod
    def load_from_disk(doc_id):
        filepath = os.path.join(DATA_DIR, f'{doc_id}.json')
        if not os.path.exists(filepath):
            return None
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                payload = json.load(f)
            doc = Document.__new__(Document)
            doc.doc_id = doc_id
            doc.data = payload.get('data', copy.deepcopy(DEFAULT_DOC))
            doc.version = payload.get('version', 0)
            doc.operations = payload.get('operations', [])
            doc.snapshots = {int(k): v for k, v in payload.get('snapshots', {}).items()}
            if 0 not in doc.snapshots:
                doc.snapshots = {0: copy.deepcopy(doc.data)}
            doc.clients = {}
            doc.last_heartbeat = {}
            return doc
        except Exception as e:
            print(f'[PERSIST] Failed to load doc {doc_id}: {e}')
            return None

    def get_state_at(self, target_version):
        if target_version < 0 or target_version > self.version:
            return None
        nearest_snapshot = max(v for v in self.snapshots if v <= target_version)
        state = copy.deepcopy(self.snapshots[nearest_snapshot])
        ops_to_replay = [op for op in self.operations
                         if nearest_snapshot < op.get('applied_version', 0) <= target_version]
        ops_to_replay.sort(key=lambda x: x.get('applied_version', 0))
        for op in ops_to_replay:
            op_type = op['type']
            if op_type in ('update', 'replace'):
                _set_value_inplace(state, op['path'], op['value'])
            elif op_type == 'add':
                _add_node_inplace(state, op['path'], op['key'], op['value'])
            elif op_type == 'delete':
                _delete_path_inplace(state, op['path'])
            elif op_type == 'move':
                val = _get_value_at(state, op['from_path'])
                _delete_path_inplace(state, op['from_path'])
                _set_value_inplace(state, op['to_path'], val)
        return state

    def reverse_operation(self, op):
        if op['type'] == 'update':
            if 'old_value' in op:
                return {
                    'type': 'update',
                    'path': op['path'],
                    'value': op['old_value'],
                    'user_id': 'system',
                    'reverse_of': op.get('applied_version', 0)
                }
        elif op['type'] == 'add':
            return {
                'type': 'delete',
                'path': list(op['path']) + [op['key']],
                'user_id': 'system',
                'reverse_of': op.get('applied_version', 0)
            }
        elif op['type'] == 'delete':
            if 'old_value' in op:
                old_path = list(op['path'])
                key = old_path.pop()
                return {
                    'type': 'add',
                    'path': old_path,
                    'key': key,
                    'value': op['old_value'],
                    'user_id': 'system',
                    'reverse_of': op.get('applied_version', 0)
                }
        elif op['type'] == 'move':
            if 'from_path' in op and 'to_path' in op:
                return {
                    'type': 'move',
                    'from_path': op['to_path'],
                    'to_path': op['from_path'],
                    'user_id': 'system',
                    'reverse_of': op.get('applied_version', 0)
                }
        return None

    def rollback_to(self, target_version):
        if target_version > self.version or target_version < 0:
            return False
        nearest_snapshot = max(v for v in self.snapshots if v <= target_version)
        self.data = copy.deepcopy(self.snapshots[nearest_snapshot])
        self.version = nearest_snapshot
        ops_to_replay = [op for op in self.operations
                         if nearest_snapshot < op.get('applied_version', 0) <= target_version]
        ops_to_replay.sort(key=lambda x: x.get('applied_version', 0))
        for op in ops_to_replay:
            op_type = op['type']
            if op_type == 'update':
                self.set_value(op['path'], op['value'])
            elif op_type == 'add':
                self.add_node(op['path'], op['key'], op['value'])
            elif op_type == 'delete':
                self.delete_path(op['path'])
            elif op_type == 'move':
                self.move_node(op['from_path'], op['to_path'])
            self.version += 1
        self.operations = [op for op in self.operations
                           if op.get('applied_version', 0) <= target_version]
        if self.version % 5 == 0 or self.version < 5:
            self.snapshots[self.version] = copy.deepcopy(self.data)
        self._save_to_disk()
        return True

    def register_client(self, client_id, user_name, role='editor'):
        self.clients[client_id] = {
            'user_name': user_name,
            'role': role,
            'connected_at': time.time(),
            'last_version': 0
        }
        self.last_heartbeat[client_id] = time.time()

    def unregister_client(self, client_id):
        self.clients.pop(client_id, None)
        self.last_heartbeat.pop(client_id, None)

    def update_heartbeat(self, client_id):
        self.last_heartbeat[client_id] = time.time()

    def get_disconnected_clients(self):
        now = time.time()
        disconnected = []
        for cid, last_hb in list(self.last_heartbeat.items()):
            if now - last_hb > HEARTBEAT_TIMEOUT:
                disconnected.append(cid)
        return disconnected


def get_or_create_document(doc_id):
    if doc_id not in DOCUMENTS:
        loaded = Document.load_from_disk(doc_id)
        if loaded:
            DOCUMENTS[doc_id] = loaded
        else:
            DOCUMENTS[doc_id] = Document(doc_id)
    return DOCUMENTS[doc_id]


def paths_overlap(p1, p2):
    if not p1 or not p2:
        return False
    min_len = min(len(p1), len(p2))
    for i in range(min_len):
        if p1[i] != p2[i]:
            return False
    return True


def is_ancestor_path(maybe_ancestor, target):
    if len(maybe_ancestor) >= len(target):
        return False
    for i, key in enumerate(maybe_ancestor):
        if target[i] != key:
            return False
    return True


def transform(op1, op2):
    transformed = copy.deepcopy(op1)

    t1, t2 = op1['type'], op2['type']

    if t1 in ('update', 'replace') and t2 in ('update', 'replace'):
        if op1['path'] == op2['path']:
            transformed['_conflict'] = True
            transformed['_conflict_with'] = op2.get('applied_version')
            return transformed

    elif t1 in ('update', 'replace') and t2 == 'delete':
        if op1['path'] == op2['path'] or is_ancestor_path(op2['path'], op1['path']):
            return None

    elif t1 == 'delete' and t2 == 'delete':
        if op1['path'] == op2['path']:
            return None

    elif t1 == 'add' and t2 == 'delete':
        full_path = list(op1['path']) + [op1['key']]
        if full_path == op2['path'] or is_ancestor_path(op2['path'], full_path):
            return None

    elif t1 == 'add' and t2 == 'add':
        full_path1 = list(op1['path']) + [op1['key']]
        full_path2 = list(op2['path']) + [op2['key']]
        if full_path1 == full_path2:
            return None

    elif t1 == 'move':
        if t2 == 'move':
            if op1['from_path'] == op2['from_path'] and op1['to_path'] == op2['to_path']:
                return None

    return transformed


def maybe_transform_path(path, deleted_path):
    if is_ancestor_path(deleted_path, path):
        return None
    return path


@socketio.on('connect')
def handle_connect():
    emit('connected', {'sid': request.sid})


@socketio.on('join')
def handle_join(data):
    doc_id = data.get('doc_id', 'default')
    user_name = data.get('user_name', 'Anonymous')
    role = data.get('role', 'editor')
    doc = get_or_create_document(doc_id)
    join_room(doc_id)
    doc.register_client(request.sid, user_name, role)

    emit('sync_full', {
        'data': doc.data,
        'version': doc.version,
        'operations': doc.operations,
        'clients': {k: v for k, v in doc.clients.items() if k != request.sid},
        'user_name': user_name,
        'role': role,
        'doc_id': doc_id
    })

    role_str = '观察者 ' if role == 'observer' else ''
    emit('user_joined', {
        'user_id': request.sid,
        'user_name': user_name,
        'role': role,
        'clients': {k: v for k, v in doc.clients.items()}
    }, room=doc_id, include_self=False)

    emit('operation_log', {
        'message': f'{role_str}用户 {user_name} 加入了文档',
        'type': 'system',
        'timestamp': time.time()
    }, room=doc_id)


@socketio.on('submit_operation')
def handle_operation(data):
    doc_id = data.get('doc_id', 'default')
    operation = data.get('operation', {})
    conflict_strategy = data.get('conflict_strategy', 'last_writer_wins')
    doc = get_or_create_document(doc_id)

    client_info = doc.clients.get(request.sid, {})
    if client_info.get('role') == 'observer':
        emit('operation_rejected', {
            'original_op': operation,
            'reason': '观察者模式，无法编辑',
            'version': doc.version
        })
        return

    op_doc_version = operation.get('doc_version', 0)
    operation['user_id'] = request.sid
    operation['timestamp'] = time.time()
    operation['user_name'] = client_info.get('user_name', 'Unknown')

    if op_doc_version == doc.version:
        old_value = None
        if operation['type'] in ('update', 'delete'):
            old_value = doc.get_value(operation['path'])
        if operation['type'] == 'move':
            old_value = doc.get_value(operation['from_path'])
        if operation['type'] == 'update' and not operation.get('path'):
            operation['type'] = 'replace'
            operation['description'] = '整份文档替换'
        if old_value is not None:
            operation['old_value'] = old_value

        doc.apply_operation(operation)
        doc.clients[request.sid]['last_version'] = doc.version
        if operation['type'] in ('update', 'replace'):
            operation['new_value'] = operation['value']

        emit('operation_applied', {
            'operation': operation,
            'version': doc.version,
            'data': doc.data,
            'user_id': request.sid
        }, room=doc_id)

        log_data = {
            'message': f"{operation['user_name']} 执行了 {_op_desc(operation)}",
            'type': 'info',
            'user_name': operation['user_name'],
            'op_type': operation['type'],
            'timestamp': time.time(),
            'version': doc.version
        }
        if operation['type'] == 'update':
            log_data['old_value'] = _serializable(operation.get('old_value'))
            log_data['new_value'] = _serializable(operation.get('new_value'))
            if old_value is not None:
                log_data['message'] += f': {_serializable(old_value)} → {_serializable(operation["value"])}'
        elif operation['type'] == 'delete':
            log_data['old_value'] = _serializable(operation.get('old_value'))
        elif operation['type'] == 'add':
            log_data['new_value'] = _serializable(operation.get('value'))
        emit('operation_log', log_data, room=doc_id)

    else:
        if op_doc_version < doc.version:
            resolved = _resolve_conflict(doc, operation, op_doc_version, conflict_strategy)
            if resolved:
                has_conflict = resolved.get('_conflict', False)
                conflict_ver = resolved.get('_conflict_with', None)

                old_value = None
                if resolved['type'] in ('update', 'delete'):
                    old_value = doc.get_value(resolved['path'])
                if resolved['type'] == 'move':
                    old_value = doc.get_value(resolved['from_path'])
                if old_value is not None:
                    resolved['old_value'] = old_value

                doc.apply_operation(resolved)
                doc.clients[request.sid]['last_version'] = doc.version
                if resolved['type'] == 'update':
                    resolved['new_value'] = resolved['value']

                emit('operation_applied', {
                    'operation': resolved,
                    'version': doc.version,
                    'data': doc.data,
                    'user_id': request.sid
                }, room=doc_id)

                log_data = {
                    'type': 'info',
                    'user_name': operation['user_name'],
                    'op_type': resolved['type'],
                    'timestamp': time.time(),
                    'version': doc.version
                }
                if resolved['type'] == 'update':
                    log_data['old_value'] = _serializable(resolved.get('old_value'))
                    log_data['new_value'] = _serializable(resolved.get('value'))
                if has_conflict:
                    log_data['message'] = f"{operation['user_name']} 的 {_op_desc(resolved)} 覆盖了版本 {conflict_ver} (后提交者获胜)"
                    if resolved['type'] == 'update':
                        log_data['message'] += f': 新值={_serializable(resolved["value"])}'
                else:
                    log_data['message'] = f"{operation['user_name']} 执行了 {_op_desc(resolved)} (已解决冲突)"
                emit('operation_log', log_data, room=doc_id)
            else:
                emit('operation_rejected', {
                    'original_op': operation,
                    'reason': '冲突无法解决，操作被丢弃',
                    'version': doc.version
                })

        elif op_doc_version > doc.version:
            emit('sync_required', {'version': doc.version})


def _resolve_conflict(doc, operation, op_doc_version, strategy):
    intermediate_ops = [op for op in doc.operations
                        if op.get('applied_version', 0) > op_doc_version
                        and op.get('applied_version', 0) <= doc.version]
    intermediate_ops.sort(key=lambda x: x.get('applied_version', 0))

    if strategy == 'last_writer_wins':
        transformed = copy.deepcopy(operation)
        for iop in intermediate_ops:
            result = transform(transformed, iop)
            if result is None:
                return None
            transformed = result
        return transformed

    elif strategy == 'auto_merge':
        transformed = copy.deepcopy(operation)
        for iop in intermediate_ops:
            result = transform(transformed, iop)
            if result is None:
                return None
            transformed = result
        return transformed

    return operation


def _op_desc(op):
    t = op.get('type', '')
    path_str = '/'.join(str(p) for p in op.get('path', []))
    if t == 'update':
        return f'更新操作 (路径: {path_str})'
    elif t == 'add':
        return f'添加节点 (路径: {path_str}/{op.get("key", "")})'
    elif t == 'delete':
        return f'删除节点 (路径: {path_str})'
    elif t == 'move':
        from_str = '/'.join(str(p) for p in op.get('from_path', []))
        to_str = '/'.join(str(p) for p in op.get('to_path', []))
        return f'移动节点 ({from_str} -> {to_str})'
    elif t == 'rollback':
        return f'回退操作 (目标版本: {op.get("target_version", "?")})'
    elif t == 'replace':
        return '整份文档替换'
    return '未知操作'


@socketio.on('heartbeat')
def handle_heartbeat(data):
    doc_id = data.get('doc_id', 'default')
    doc = get_or_create_document(doc_id)
    doc.update_heartbeat(request.sid)


@socketio.on('request_sync')
def handle_request_sync(data):
    doc_id = data.get('doc_id', 'default')
    client_version = data.get('version', 0)
    doc = get_or_create_document(doc_id)

    if client_version >= doc.version:
        emit('sync_full', {
            'data': doc.data,
            'version': doc.version,
            'operations': doc.operations,
            'clients': {k: v for k, v in doc.clients.items() if k != request.sid},
            'doc_id': doc_id
        })
        return

    missing_ops = [op for op in doc.operations
                   if op.get('applied_version', 0) > client_version]
    missing_ops.sort(key=lambda x: x.get('applied_version', 0))

    if len(missing_ops) > 20:
        emit('sync_full', {
            'data': doc.data,
            'version': doc.version,
            'operations': doc.operations,
            'clients': {k: v for k, v in doc.clients.items() if k != request.sid},
            'doc_id': doc_id
        })
    else:
        emit('sync_operations', {
            'operations': missing_ops,
            'version': doc.version,
            'data': doc.data,
            'doc_id': doc_id
        })


@socketio.on('disconnect')
def handle_disconnect():
    for doc_id, doc in list(DOCUMENTS.items()):
        if request.sid in doc.clients:
            user_name = doc.clients[request.sid].get('user_name', 'Unknown')
            doc.unregister_client(request.sid)

            emit('user_left', {
                'user_id': request.sid,
                'user_name': user_name,
                'clients': {k: v for k, v in doc.clients.items()}
            }, room=doc_id)

            emit('operation_log', {
                'message': f'用户 {user_name} 离开了文档',
                'type': 'system',
                'timestamp': time.time()
            }, room=doc_id)

            if not doc.clients:
                DOCUMENTS.pop(doc_id, None)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/doc/<doc_id>')
def get_doc(doc_id):
    doc = get_or_create_document(doc_id)
    return jsonify({
        'data': doc.data,
        'version': doc.version,
        'operations': len(doc.operations)
    })


@app.route('/api/doc/<doc_id>/export')
def export_doc(doc_id):
    doc = get_or_create_document(doc_id)
    export_data = json.dumps(doc.data, ensure_ascii=False, indent=2)
    filepath = os.path.join(os.path.dirname(__file__), f'export_{doc_id}_{int(time.time())}.json')
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(export_data)
    response = send_file(filepath, as_attachment=True,
                         download_name=f'{doc_id}_{int(time.time())}.json',
                         mimetype='application/json')
    return response


@app.route('/api/doc/<doc_id>/versions')
def get_versions(doc_id):
    doc = get_or_create_document(doc_id)
    versions = [{
        'version': 0,
        'type': 'init',
        'user_name': 'System',
        'timestamp': 0,
        'description': '初始文档状态'
    }]
    for op in doc.operations:
        versions.append({
            'version': op.get('applied_version', 0),
            'type': op.get('type'),
            'user_name': op.get('user_name', 'Unknown'),
            'timestamp': op.get('timestamp', 0),
            'description': _op_desc(op),
            'old_value': op.get('old_value')
        })
        if 'new_value' in op:
            versions[-1]['new_value'] = op['new_value']
    return jsonify({'versions': versions, 'current_version': doc.version})


@app.route('/api/doc/<doc_id>/diff/<int:v1>/<int:v2>')
def diff_versions(doc_id, v1, v2):
    doc = get_or_create_document(doc_id)
    state1 = doc.get_state_at(v1)
    state2 = doc.get_state_at(v2)
    if state1 is None or state2 is None:
        return jsonify({'error': '版本不存在'}), 404
    diffs = compute_diff(state1, state2)
    return jsonify({'diff': diffs, 'v1': v1, 'v2': v2})


@app.route('/api/doc/<doc_id>/version/<int:version>')
def version_detail(doc_id, version):
    doc = get_or_create_document(doc_id)
    state = doc.get_state_at(version)
    if state is None:
        return jsonify({'error': '版本不存在'}), 404
    meta = None
    for op in doc.operations:
        if op.get('applied_version', 0) == version:
            meta = {
                'type': op.get('type'),
                'user_name': op.get('user_name', 'Unknown'),
                'timestamp': op.get('timestamp', 0),
                'description': _op_desc(op),
                'old_value': op.get('old_value'),
                'new_value': op.get('new_value'),
                'path': op.get('path')
            }
            break
    if not meta:
        meta = {'type': 'init', 'user_name': 'System', 'timestamp': 0, 'description': '初始文档状态'}
    prev_state = doc.get_state_at(max(0, version - 1)) if version > 0 else None
    diff_from_prev = compute_diff(prev_state, state) if prev_state else None
    return jsonify({
        'data': state,
        'version': version,
        'meta': meta,
        'diff_from_prev': diff_from_prev
    })


@app.route('/api/doc/<doc_id>/audit')
def audit_operations(doc_id):
    doc = get_or_create_document(doc_id)
    user_name = request.args.get('user_name', '')
    op_type = request.args.get('op_type', '')
    from_ver = request.args.get('from_ver', type=int)
    to_ver = request.args.get('to_ver', type=int)

    filtered = []
    conflicts = []
    for op in doc.operations:
        v = op.get('applied_version', 0)
        if user_name and op.get('user_name', '') != user_name:
            continue
        if op_type and op.get('type', '') != op_type:
            continue
        if from_ver is not None and v < from_ver:
            continue
        if to_ver is not None and v > to_ver:
            continue
        entry = {
            'version': v,
            'type': op.get('type'),
            'user_name': op.get('user_name', 'Unknown'),
            'timestamp': op.get('timestamp', 0),
            'description': _op_desc(op),
            'old_value': op.get('old_value'),
            'new_value': op.get('new_value')
        }
        if op.get('_conflict_with'):
            entry['conflict'] = True
            entry['conflict_with_version'] = op.get('_conflict_with')
            entry['conflict_desc'] = f"{op.get('user_name')} 的 {_op_desc(op)} 覆盖了版本 {op.get('_conflict_with')} 的修改"
        filtered.append(entry)
        if entry.get('conflict'):
            conflicts.append(entry)

    return jsonify({
        'filtered': filtered,
        'conflicts': conflicts,
        'total': len(filtered),
        'conflict_count': len(conflicts)
    })


@app.route('/api/doc/<doc_id>/operation/<int:version>')
def get_operation_detail(doc_id, version):
    doc = get_or_create_document(doc_id)
    op = next((o for o in doc.operations if o.get('applied_version', 0) == version), None)
    if not op:
        return jsonify({'error': '操作不存在'}), 404
    detail = dict(op)
    if op.get('applied_version') > 1:
        prev_state = doc.get_state_at(version - 1)
        if prev_state and op.get('path'):
            detail['before'] = _get_value_at(prev_state, op['path'])
        current_state = doc.get_state_at(version)
        if current_state and op.get('path'):
            detail['after'] = _get_value_at(current_state, op['path'])
    return jsonify({'operation': detail})


@app.route('/api/doc/<doc_id>/rollback/<int:target_version>', methods=['POST'])
def rollback_doc(doc_id, target_version):
    doc = get_or_create_document(doc_id)
    user_id = request.args.get('sid', '')
    if user_id and doc.clients.get(user_id, {}).get('role') == 'observer':
        return jsonify({'success': False, 'error': '观察者模式，无法回退'}), 403
    old_version = doc.version
    success = doc.rollback_to(target_version)
    if success:
        doc.version += 1
        new_ver = doc.version
        doc.operations.append({
            'type': 'rollback',
            'user_name': '系统',
            'user_id': user_id or 'system',
            'timestamp': time.time(),
            'applied_version': new_ver,
            'from_version': old_version,
            'target_version': target_version,
            'description': f'回退: v{old_version} → v{target_version}'
        })
        if new_ver % 5 == 0 or new_ver < 5:
            doc.snapshots[new_ver] = copy.deepcopy(doc.data)
        doc._save_to_disk()
        socketio.emit('operation_log', {
            'message': f'文档已回退到版本 {target_version} (from v{old_version})，当前版本 v{new_ver}',
            'type': 'system',
            'timestamp': time.time()
        }, room=doc_id)
        socketio.emit('sync_full', {
            'data': doc.data,
            'version': doc.version,
            'operations': doc.operations,
            'clients': {k: v for k, v in doc.clients.items()},
            'doc_id': doc_id
        }, room=doc_id)
        return jsonify({'success': True, 'version': doc.version})
    return jsonify({'success': False, 'error': '无效的目标版本'}), 400


def heartbeat_checker():
    while True:
        time.sleep(HEARTBEAT_CHECK_INTERVAL)
        for doc_id, doc in list(DOCUMENTS.items()):
            disconnected = doc.get_disconnected_clients()
            for cid in disconnected:
                if cid in doc.clients:
                    user_name = doc.clients[cid].get('user_name', 'Unknown')
                    doc.unregister_client(cid)
                    socketio.emit('user_left', {
                        'user_id': cid,
                        'user_name': user_name,
                        'clients': {k: v for k, v in doc.clients.items()}
                    }, room=doc_id)
                    socketio.emit('operation_log', {
                        'message': f'用户 {user_name} 心跳超时，已断开连接',
                        'type': 'system',
                        'timestamp': time.time()
                    }, room=doc_id)


if __name__ == '__main__':
    heartbeat_thread = threading.Thread(target=heartbeat_checker, daemon=True)
    heartbeat_thread.start()
    socketio.run(app, host='0.0.0.0', port=5050, debug=True, allow_unsafe_werkzeug=True)