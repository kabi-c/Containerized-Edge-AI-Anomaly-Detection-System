#include <iostream> 
#include <fstream> 
#include <vector> 
#include <string> 
#include <chrono> 
#include <cmath> 
#include <sstream> 
#include <nlohmann/json.hpp> 
#include <zmq.hpp> 
#include "mqtt_publisher.hpp" 
using json = nlohmann::json; 
// Helper: load csv into matrix (rows x cols) 
static std::vector<std::vector<double>> load_csv(const std::string &path) { 
std::ifstream f(path); 
std::vector<std::vector<double>> mat; 
std::string line; 
while(std::getline(f, line)) { 
std::stringstream ss(line); 
std::vector<double> row; 
std::string token; 
while(std::getline(ss, token, ',')) row.push_back(std::stod(token)); 
if(!row.empty()) mat.push_back(row); 
} 
return mat; 
} 
static std::vector<double> load_vec_json(const std::string &path_mean, const 
std::string &path_scale) { 
std::ifstream f(path_mean); 
json jm; f >> jm; 
std::vector<double> mean = jm["mean"].get<std::vector<double>>(); 
// load scale 
std::ifstream fs(path_scale); 
json js; fs >> js; 
std::vector<double> scale = js["scale"].get<std::vector<double>>(); 
// store interleaved [mean..., scale...] or return concatenate? We'll return mean 
then append scale. 
mean.insert(mean.end(), scale.begin(), scale.end()); 
return mean; 
22 
} 
static double l2_norm(const std::vector<double> &a, const std::vector<double> 
&b) { 
double s = 0.0; 
for(size_t i=0;i<a.size();++i) { 
double d = a[i]-b[i]; 
s += d*d; 
} 
return std::sqrt(s); 
} 
// matrix-vector multiply: out = mat * vec  (mat rows x cols) 
static std::vector<double> matvec(const std::vector<std::vector<double>> &mat, 
const std::vector<double> &vec) { 
size_t rows = mat.size(), cols = mat[0].size(); 
std::vector<double> out(rows, 0.0); 
for(size_t i=0;i<rows;++i) { 
double s=0.0; 
for(size_t j=0;j<cols;++j) s += mat[i][j] * vec[j]; 
out[i]=s; 
} 
return out; 
} 
// transpose mat^T 
static 
std::vector<std::vector<double>> 
std::vector<std::vector<double>> &m) { 
size_t r = m.size(), c = m[0].size(); 
std::vector<std::vector<double>> t(c, std::vector<double>(r)); 
for(size_t i=0;i<r;++i) for(size_t j=0;j<c;++j) t[j][i]=m[i][j]; 
return t; 
} 
transpose(const 
23 
int main(int argc, char** argv) { 
// Config (can be env vars) 
std::string zmq_addr = "tcp://127.0.0.1:5556"; 
std::string mqtt_uri = "ssl://127.0.0.1:8883"; // use ssl or tcp 
std::string mqtt_client = "edge_pca_01"; 
std::string mqtt_topic = "edge/alerts"; 
std::string scaler_file = "models/scaler.json"; 
std::string pca_comp = "models/pca_components.csv"; 
std::string pca_mean = "models/pca_mean.csv"; 
std::string threshold_file = "models/threshold.json"; 
bool use_tls = false; 
std::string ca_file="", client_cert="", client_key=""; 
// Load scaler & threshold 
std::ifstream tfs(threshold_file); json jthr; tfs >> jthr; double threshold = 
jthr["threshold"].get<double>(); 
// Load scaler.json 
std::ifstream sfs(scaler_file); json js; sfs >> js; 
std::vector<double> mean = js["mean"].get<std::vector<double>>(); 
std::vector<double> scale = js["scale"].get<std::vector<double>>(); 
// Load PCA matrix and pca_mean 
auto components = load_csv(pca_comp); // rows = components, cols = features 
auto pca_mean_vec = load_csv(pca_mean)[0]; 
// Precompute transposed components for reconstruction: comp^T 
auto compT = transpose(components); 
// ZeroMQ setup 
zmq::context_t ctx(1); 
zmq::socket_t sub(ctx, zmq::socket_type::sub); 
sub.connect(zmq_addr); 
const char* topic = "sensor"; 
24 
25 
 
    sub.setsockopt(ZMQ_SUBSCRIBE, topic, 6); 
 
    // MQTT publisher wrapper 
    MqttPublisher publisher(mqtt_uri, mqtt_client, mqtt_topic); 
    mqtt::connect_options opts; 
    if(use_tls) opts = publisher.create_tls_options(ca_file, client_cert, client_key, 
true); 
    if(!publisher.connect(opts)) { 
        std::cerr << "MQTT connect failed; continuing and will drop publishes\n"; 
    } 
 
    std::cout << "Listening on " << zmq_addr << " threshold=" << threshold << "\n"; 
 
    while(true) { 
        zmq::message_t message; 
        sub.recv(message); 
        std::string s((char*)message.data(), message.size()); 
        auto pos = s.find(' '); 
        if(pos==std::string::npos) continue; 
        std::string payload = s.substr(pos+1); 
 
        // parse JSON 
        json jsn; 
        try { jsn = json::parse(payload); } 
        catch(...) { continue; } 
 
        // Build feature vector (order must match training: temp, humidity, motion) 
        std::vector<double> x_raw = { jsn["temp"].get<double>(), 
jsn["humidity"].get<double>(), jsn["motion"].get<double>() }; 
 
        // scale: x_scaled = (x_raw - mean) / scale 
        std::vector<double> x_scaled(3); 
        for(size_t i=0;i<3;++i) x_scaled[i] = (x_raw[i] - mean[i]) / (scale[i] + 1e-12); 
 
26 
 
        // PCA transform: z = C * (x_scaled - pca_mean) 
        std::vector<double> centered(3); 
        for(size_t i=0;i<3;++i) centered[i] = x_scaled[i] - pca_mean_vec[i]; 
        std::vector<double> z = matvec(components, centered); 
 
        // Reconstruct: x_rec = pca_mean + C^T * z 
        std::vector<double> rec = matvec(compT, z); 
        for(size_t i=0;i<3;++i) rec[i] += pca_mean_vec[i]; 
 
        // Compute L2 error between x_scaled and rec 
        double err = l2_norm(x_scaled, rec); 
 
        if(err > threshold) { 
            json alert = { 
                {"device","edge01"}, 
                {"ts", 
(long)std::chrono::system_clock::to_time_t(std::chrono::system_clock::now())}, 
                {"features", x_raw}, 
                {"error", err} 
            }; 
            std::string alert_s = alert.dump(); 
            // publish alert (non-blocking) 
            publisher.publish(alert_s, 1, false); 
            std::cout << "ALERT published: err=" << err << " payload=" << alert_s << 
"\n"; 
        } 
    } 
    return 0; 
}
