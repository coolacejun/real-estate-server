# Flutter → Server 마이그레이션 가이드

이 문서는 기존 Firebase 기반 코드를 자체 서버로 전환하기 위한 작업 순서와 샘플 구현을 제공합니다.

## 1. 의존성 정리

`pubspec.yaml`에서 Firebase 관련 의존성 제거 또는 비활성화를 권장합니다.

- `firebase_core`
- `cloud_firestore`
- `firebase_remote_config`

이미 `http` 패키지를 쓰고 있으므로 추가 설치 없이 진행 가능합니다.

## 2. 서비스 교체 전략

현재 구조
- `FirebaseDataService`를 `AppService`에서 사용

목표 구조
- `ServerDataService`를 신설하고 `AppService`에서 사용
- Firebase SDK는 제거 가능

## 3. ServerDataService 예시

아래 파일을 새로 만들고, `AppService`에서 주입 대상을 교체합니다.

파일: `lib/services/server_data_service.dart`

```dart
import 'dart:convert';
import 'package:http/http.dart' as http;

class ServerDataService {
  final String baseUrl;
  final http.Client _client;

  ServerDataService({
    this.baseUrl = 'https://api.building-land.com',
    http.Client? client,
  }) : _client = client ?? http.Client();

  Future<bool> checkAppVersion({required bool isAndroid}) async {
    final platform = isAndroid ? 'android' : 'ios';
    final uri = Uri.parse('$baseUrl/v1/app-config?platform=$platform');
    final res = await _client.get(uri);
    if (res.statusCode != 200) return false;
    final jsonBody = jsonDecode(res.body) as Map<String, dynamic>;
    if (jsonBody['ok'] != true) return false;
    final data = jsonBody['data'] as Map<String, dynamic>;
    final minRequired = data['min_required_version'] as int? ?? 0;
    final currentBuildNumber = 0; // TODO: package_info_plus로 대체
    return minRequired > currentBuildNumber;
  }

  Future<Map<String, dynamic>> getSimpleData(String docName) async {
    final uri = Uri.parse('$baseUrl/v1/simple-data/$docName');
    final res = await _client.get(uri);
    if (res.statusCode != 200) return {};
    final body = jsonDecode(res.body) as Map<String, dynamic>;
    return (body['data'] as Map<String, dynamic>?) ?? {};
  }

  Future<String> getData(String collectionName, String pnu) async {
    final uri = Uri.parse('$baseUrl/v1/data/$collectionName/$pnu?format=compressed');
    final res = await _client.get(uri);
    if (res.statusCode != 200) return '';
    final body = jsonDecode(res.body) as Map<String, dynamic>;
    if (body['ok'] != true) return '';
    final data = body['data'] as Map<String, dynamic>;
    final parts = (data['parts'] as List?)?.cast<String>() ?? [];
    if (parts.isEmpty) return '';
    return parts.join();
  }

  Future<List<String>> getTileData(String root, String parent, String id) async {
    final uri = Uri.parse('$baseUrl/v1/tile/$root/$parent/$id');
    final res = await _client.get(uri);
    if (res.statusCode != 200) return [];
    final body = jsonDecode(res.body) as Map<String, dynamic>;
    if (body['ok'] != true) return [];
    final data = body['data'] as Map<String, dynamic>;
    final parts = (data['parts'] as List?)?.cast<String>() ?? [];
    return parts;
  }

  Future<Map<String, dynamic>> getPnuPointData(String pnu) async {
    final uri = Uri.parse('$baseUrl/v1/pnu/$pnu/polygon?format=points');
    final res = await _client.get(uri);
    if (res.statusCode != 200) return {};
    final body = jsonDecode(res.body) as Map<String, dynamic>;
    return (body['data'] as Map<String, dynamic>?) ?? {};
  }

  Future<List<dynamic>> getBuildingGeo(String pnu) async {
    final uri = Uri.parse('$baseUrl/v1/geo/building?pnu=$pnu');
    final res = await _client.get(uri);
    if (res.statusCode != 200) return [];
    final body = jsonDecode(res.body) as Map<String, dynamic>;
    return (body['data'] as List?) ?? [];
  }

  Future<Map<String, dynamic>> getBuildingViolationInfo(String pnu) async {
    final uri = Uri.parse('$baseUrl/v1/geo/building/violations?pnu=$pnu');
    final res = await _client.get(uri);
    if (res.statusCode != 200) return {};
    final body = jsonDecode(res.body) as Map<String, dynamic>;
    return (body['data'] as Map<String, dynamic>?) ?? {};
  }

  Future<Map<String, dynamic>> getLandGeo(String pnu) async {
    final uri = Uri.parse('$baseUrl/v1/geo/land/$pnu');
    final res = await _client.get(uri);
    if (res.statusCode != 200) return {};
    final body = jsonDecode(res.body) as Map<String, dynamic>;
    return (body['data'] as Map<String, dynamic>?) ?? {};
  }

  Future<List<dynamic>> getLandGeoForMultiplePrefixes(List<String> prefixes) async {
    final uri = Uri.parse('$baseUrl/v1/geo/land/features');
    final res = await _client.post(uri,
        headers: {'Content-Type': 'application/json'},
        body: jsonEncode({'prefixes': prefixes}));
    if (res.statusCode != 200) return [];
    final body = jsonDecode(res.body) as Map<String, dynamic>;
    return (body['data'] as List?) ?? [];
  }
}
```

## 4. AppService 교체 포인트

`AppService`에서 `FirebaseDataService` 대신 `ServerDataService`를 주입합니다.

- 기존: `final _firebaseDataService = locator<FirebaseDataService>();`
- 변경: `final _serverDataService = locator<ServerDataService>();`

그리고 모든 호출부를 서버 기반 메서드로 교체합니다.

## 5. Firebase 제거 체크리스트

- `Firebase.initializeApp()` 제거
- Remote Config 관련 코드 제거
- Firestore 관련 코드 제거
- `firebase_core`, `cloud_firestore`, `firebase_remote_config` 제거

## 6. 권장 개선

- 서버 응답에 `cache` 플래그 추가
- `format=lines` 옵션은 서버에서 미리 분해해서 반환
- `data`와 `tile` 응답에 `source` 필드 추가 (cache|db)

