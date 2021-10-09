#include <iostream>
#include <cstdio>
#include <algorithm>
#include <cstring>
#include <cmath>
#include <map>
#include <vector>
#include <set>
#include <queue>
#include <stack>
#include <sstream>
#include <unordered_map>
#define ll long long
#define ull unsigned long long
#define re return
#define pb push_back
#define Endl "\n"
#define endl "\n"
#define x first
#define y second

using namespace std;

typedef pair<int, int> PII;

const int N = 2e5 + 10;
const int M = 1e5 + 10;
const int mod = 1e9 + 7;
const int INF = 0x3f3f3f3f;

int dx[4] = {-1,0,1,0};
int dy[4] = {0,1,0,-1};

int T;
int n;
int a[N];
int tr[N];
vector<int> alls; // 存储所有待离散化的值

// 二分求出x对应的离散化的值
int find(int x){
    return lower_bound(alls.begin(), alls.end(), x) - alls.begin() + 1; // 下标从1开始
}

int lb(int x){
    return x & -x;
}

void modify(int p, int v){
    for(; p < N; p += lb(p)){
        tr[p] += v;
    }
}

int query(int p){
    int res = 0;
    for(; p; p -= lb(p)){
        res += tr[p];
    }
    return res;
}

void solve(){
    memset(tr, 0, sizeof(tr));
    alls.clear();

    cin >> n;
    for (int i = 1; i <= n; i++){
        cin >> a[i];
        alls.pb(a[i]);
    }

    sort(alls.begin(), alls.end());                         // 将所有值排序
    alls.erase(unique(alls.begin(), alls.end()), alls.end());   // 去掉重复元素

    for (int i = 1; i <= n; i++){
        a[i] = find(a[i]);
    }

    ll ans = 0;
    for (int i = 1; i <= n; i++){
        int l = query(a[i] - 1);
        int r = query(N - 1) - query(a[i]);
        if(l <= r){
            ans += l;
        }
        else
            ans += r;
        modify(a[i], 1);
    }

    cout << ans << Endl;
}

int main(){
    cin >> T;
    while(T--){
        solve();
    }
    return 0;
}