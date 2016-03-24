var WildEmitter = require("wildemitter");
var adapter = require('webrtc-adapter');
console.log(adapter)
module.exports = WildPeerConnection;
if (window)
    window.WildPeerConnection = WildPeerConnection;

function WildPeerConnection(ref, remoteRef, config) { //清空ref数据
    this.peerConnection = null;
    this.ref = ref;
    this.signalRef = this.ref.child("signal");
    this.offerRef = this.ref.child("signal/offer");
    this.answerRef = this.ref.child("signal/answer")
    this.candidateRef = this.ref.child("candidate");
    
    this.config = config||{};
    
    //this.config['bundlePolicy'] = 'max-bundle';
    this.remoteRef = remoteRef;
    this.initPeerConnection_(this.config);
    this.bufferedNewCandidate = {};
    this.tick = null;
}
WildEmitter.mixin(WildPeerConnection);
WildPeerConnection.prototype.initPeerConnection_ = function (config) {
    var peerConnection = new RTCPeerConnection();
    this.setPeerConnection(peerConnection);
}
WildPeerConnection.prototype.setPeerConnection = function (peerConnection) {

    this.peerConnection = peerConnection;
    //同步状态

    this.iceConnectionState = peerConnection.iceConnectionState;
    this.localDescription = peerConnection.localDescription
    this.iceGatheringState = peerConnection.iceGatheringState;
    this.peerIdentity = peerConnection.peerIdentity;
    this.remoteDescription = peerConnection.remoteDescription;
    this.signalingState = peerConnection.signalingState;
    this.ref.child("iceConnectionState").set(this.iceConnectionState);
    peerConnection.oniceconnectionstatechange = function (ev) {
        this.iceConnectionState = peerConnection.iceConnectionState;
        this.ref.child("iceConnectionState").set(this.iceConnectionState);
        if (this.iceConnectionState == 'failed' || this.iceConnectionState == 'disconnected') {
            this.offerRef.off('value');
            this.answerRef.off('value');
            this.candidateRef.off('child_added');
            this.ref.remove();
            clearInterval(this.tick);
            this.tick = null;
            this.emit("disconnected");
            
        }
        if (this.iceConnectionState == 'connected') {
            this.emit('connected');
        }

    }.bind(this);

    peerConnection.onpeeridentity = function (ev) {
        this.peerIdentity = peerConnection.peerIdentity;
        this.emit("peeridentity", this.peerIdentity);
    }
    peerConnection.onsignalingstatechange = function (ev) {
        this.signalingState = peerConnection.signalingState;
    }.bind(this);
    peerConnection.onicegatheringstatechange = function () {
        this.iceGatheringState = this.peerConnection.iceGatheringState;
        console.log("icegatheringstate ", this.iceGatheringState);
    }.bind(this);

    peerConnection.onidentityresult = function (ev) {
        this.emit("identityresult", ev);
    }.bind(this)
    peerConnection.onidpassertionerror = function (ev) {
        this.emit("idpassertionerror", ev);
    }.bind(this)
    peerConnection.onidpvalidationerror = function (ev) {
        this.emit("idpvalidationerror", ev);
    }.bind(this);
    peerConnection.onnegotiationneeded = function (ev) {
        this.sendOffer_(function (err) {

        }.bind(this));
    }.bind(this);
    peerConnection.onremovestream = function (ev) {
        this.emit("removestream", ev.stream);
    }.bind(this);
    peerConnection.onaddstream = function (ev) {
        this.emit("addstream", ev.stream);
    }.bind(this);
    
    peerConnection.onicecandidate = function (ev) {
        if (ev.candidate == null) {
            return;
        }
        var data = JSON.stringify(ev.candidate);
        
        var ref = this.remoteRef.child("candidate").push();
        var key = ref.key();
        this.bufferedNewCandidate[key] = data;
        
    }.bind(this);

    this.offerRef.on('value', this.offerCb_, this);
    this.answerRef.on('value', this.answerCb_, this);

    this.ref.onDisconnect().remove();
    this.tick = setInterval(function(){
        if(Object.keys(this.bufferedNewCandidate).length > 0){
            this.remoteRef.child("candidate").update(this.bufferedNewCandidate);
            this.bufferedNewCandidate = {};
        }
    }.bind(this),1000);

}

WildPeerConnection.prototype.offerCb_ = function (snapshot) {
    var offer = snapshot.val();
    if (offer != null && this.signalingState == 'stable') {
        this.lastOffer = offer;
        //别人给我发offer

        //回answer 并且set remoteref
        var desc = new RTCSessionDescription(JSON.parse(offer));
        this.peerConnection.setRemoteDescription(desc, function () {
            this.sendAnswer_(function (err) {
                console.error(err);
            });
            //listen to candidate
            this.candidateRef.on("child_added", this.candidateCb_, this);
        }.bind(this), function (err) {
            console.error(err);

        });

    }
}
WildPeerConnection.prototype.answerCb_ = function (snapshot) {
    var answer = snapshot.val();
    if (answer != null && this.signalingState == 'have-local-offer') {
        this.lastAnswer = answer;
        var desc = new RTCSessionDescription(JSON.parse(answer));
        this.peerConnection.setRemoteDescription(desc, function () {
            //listen to candidate
            this.candidateRef.on("child_added", this.candidateCb_, this);
        }.bind(this), function (err) {
            console.error(err);
        });
    }
}
WildPeerConnection.prototype.candidateCb_ = function (snap) {
    var sdp = JSON.parse(snap.val());
    if (sdp != null) {
        var candidate = new RTCIceCandidate(sdp);
            this.peerConnection.addIceCandidate(candidate, function () {
            }, function (err) {
                console.log(err);
            })
    }
}
WildPeerConnection.prototype.sendOffer_ = function (cb) {
    this.peerConnection.createOffer(function (desc) {
        this.peerConnection.setLocalDescription(desc, function () {
            this.remoteRef.child("signal/offer")
                .set(JSON.stringify(desc), function (err) {
                    if (err) {
                        cb(err);
                    }
                    else {
                        cb(null);
                    }
                }.bind(this));
        }.bind(this), function (err) {
            cb(err)
        });
    }.bind(this), function (err) {
        cb(err);
    })
}
WildPeerConnection.prototype.sendAnswer_ = function (cb) {
    this.peerConnection.createAnswer(function (desc) {
        this.peerConnection.setLocalDescription(desc, function () {
            this.remoteRef.child("signal/answer").set(JSON.stringify(desc), function (err) {
                if (err) {
                    cb(err);
                }
                else {
                    cb(null);
                }
            });
        }.bind(this), function (err) {
            cb(err);
        });
    }.bind(this), function (err) {
        cb(err);
    })
}
WildPeerConnection.prototype.addStream = function (stream) {
    if (this.peerConnection == null) {
        console.error(new Error("peerConnection not set"))
    }
    this.peerConnection.addStream(stream);
}

WildPeerConnection.prototype.removeStream = function (stream) {
    this.peerConnection.removeStream(stream);
}

WildPeerConnection.prototype.close = function () {
    this.peerConnection.close();
}
