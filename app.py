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
        if op_type == 'update':
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
        if target_version in self.snapshots:
            self.data = copy.deepcopy(self.snapshots[target_version])
            self.version = target_version
            self.operations = [op for op in self.operations
                               if op.get('applied_version', 0) <= target_version]
            return True
        ops_to_reverse = [op for op in self.operations
                          if op.get('applied_version', 0) > target_version]
        ops_to_reverse.sort(key=lambda x: x.get('applied_version', 0), reverse=True)
        for op in ops_to_reverse:
            rev = self.reverse_operation(op)
            if rev:
                self.apply_operation(rev)
                self.version += 1
        return True

    def register_client(self, client_id, user_name):
        self.clients[client_id] = {
            'user_name': user_name,
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

    if t1 == 'update' and t2 == 'update':
        if op1['path'] == op2['path']:
            return None

    elif t1 == 'update' and t2 == 'delete':
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
    doc = get_or_create_document(doc_id)
    join_room(doc_id)
    doc.register_client(request.sid, user_name)

    emit('sync_full', {
        'data': doc.data,
        'version': doc.version,
        'operations': doc.operations,
        'clients': {k: v for k, v in doc.clients.items() if k != request.sid},
        'user_name': user_name,
        'doc_id': doc_id
    })

    emit('user_joined', {
        'user_id': request.sid,
        'user_name': user_name,
        'clients': {k: v for k, v in doc.clients.items()}
    }, room=doc_id, include_self=False)

    emit('operation_log', {
        'message': f'用户 {user_name} 加入了文档',
        'type': 'system',
        'timestamp': time.time()
    }, room=doc_id)


@socketio.on('submit_operation')
def handle_operation(data):
    doc_id = data.get('doc_id', 'default')
    operation = data.get('operation', {})
    conflict_strategy = data.get('conflict_strategy', 'last_writer_wins')
    doc = get_or_create_document(doc_id)

    op_doc_version = operation.get('doc_version', 0)
    operation['user_id'] = request.sid
    operation['timestamp'] = time.time()
    operation['user_name'] = doc.clients.get(request.sid, {}).get('user_name', 'Unknown')

    if op_doc_version == doc.version:
        old_value = None
        if operation['type'] in ('update', 'delete'):
            old_value = doc.get_value(operation['path'])
        if operation['type'] == 'move':
            old_value = doc.get_value(operation['from_path'])
        if old_value is not None:
            operation['old_value'] = old_value

        doc.apply_operation(operation)
        doc.clients[request.sid]['last_version'] = doc.version

        emit('operation_applied', {
            'operation': operation,
            'version': doc.version,
            'data': doc.data,
            'user_id': request.sid
        }, room=doc_id)

        emit('operation_log', {
            'message': f"{operation['user_name']} 执行了 {_op_desc(operation)}",
            'type': 'info',
            'user_name': operation['user_name'],
            'op_type': operation['type'],
            'timestamp': time.time(),
            'version': doc.version
        }, room=doc_id)

    else:
        if op_doc_version < doc.version:
            resolved = _resolve_conflict(doc, operation, op_doc_version, conflict_strategy)
            if resolved:
                old_value = None
                if resolved['type'] in ('update', 'delete'):
                    old_value = doc.get_value(resolved['path'])
                if resolved['type'] == 'move':
                    old_value = doc.get_value(resolved['from_path'])
                if old_value is not None:
                    resolved['old_value'] = old_value

                doc.apply_operation(resolved)
                doc.clients[request.sid]['last_version'] = doc.version

                emit('operation_applied', {
                    'operation': resolved,
                    'version': doc.version,
                    'data': doc.data,
                    'user_id': request.sid
                }, room=doc_id)

                emit('operation_log', {
                    'message': f"{operation['user_name']} 执行了 {_op_desc(resolved)} (已解决冲突)",
                    'type': 'info',
                    'user_name': operation['user_name'],
                    'op_type': resolved['type'],
                    'timestamp': time.time(),
                    'version': doc.version
                }, room=doc_id)
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
    versions = []
    for op in doc.operations:
        versions.append({
            'version': op.get('applied_version', 0),
            'type': op.get('type'),
            'user_name': op.get('user_name', 'Unknown'),
            'timestamp': op.get('timestamp', 0),
            'description': _op_desc(op)
        })
    return jsonify({'versions': versions, 'current_version': doc.version})


@app.route('/api/doc/<doc_id>/rollback/<int:target_version>', methods=['POST'])
def rollback_doc(doc_id, target_version):
    doc = get_or_create_document(doc_id)
    success = doc.rollback_to(target_version)
    if success:
        socketio.emit('operation_log', {
            'message': f'文档已回退到版本 {target_version}',
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