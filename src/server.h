#ifndef GGRPC_SERVER_H_INCLUDED
#define GGRPC_SERVER_H_INCLUDED

namespace ggrpc {

/*
template<class W, class R>
class ServerReaderWriterHandler {
public:
  ServerReaderWriterHandler(grpc::CompletionQueue* cq) {
  }

  void Init(std::function<> create) {
    create_ = create;
    Request(&ctx_, &streamer_, cq_, &acceptor_thunk_);
  }

  void PreceedToAccept() {
    create_(cq_);
    OnAccept();
    streamer_.Read(&request_, &read_thunk_);
  }

  void Write(W resp) {
  }
  void WritesDone() {
  }
};

class RunJobHandler : public ServerReaderWriterHandler<cattleshed::RunJobResponse, cattleshed::RunJobRequest> {
public:
  RunJobHandler(grpc::CompletionQueue* cq, cattleshed::Cattleshed::AsyncService* service) : base(cq), service_(service) { }

  void Request(grpc::ServerContext* ctx,
        grpc::ServerAsyncReaderWriter<cattleshed::RunJobResponse, cattleshed::RunJobRequest>* streamer,
        grpc::CompletionQueue* cq,
        void* tag) override {
    service_->RequestRunJob(ctx, streamer, cq, cq, tag);
  }

  void OnAccept() override {
  }

  void OnRead(cattleshed::RunJobRequest req) override {
    Write(resp);
  }
  void OnDone() override {
    Write(resp);
    WritesDone(grpc::Status::OK);
  }
};

auto accept = []() {
  auto read = [&context_](cattleshed::RunJobRequest req) {
    context_->Write(resp);
    context_.reset();
  };
  auto done = [&context_]() {
    context_->Write(resp);
    context_->WritesDone(grpc::Status::OK);
  };

  return std::unique_ptr<>(new ServerReaderWriterHandler<cattleshed::RunJobResponse, cattleshed::RunJobRequest>(read, done));
};

auto done2 = [](std::unique_ptr<Context<cattleshed::GetVersionResponse>> context) {
  context->Finish(resp, grpc::Status::OK);
};

class CattleshedServer {
  cattleshed::Cattleshed::AsyncService service_;
  ggrpc::Server server_;

  void Start() {
    auto create = [this](grpc::CompletionQueue* cq) {
      return std::unique_ptr<RunJobHandler>(new RunJobHandler(cq, &service_));
    }

    server.AddReaderWriterHandler<RunJobHandler>(create);
    server.AddResponseWriterHandler<cattleshed::GetVersionRequest, cattleshed::GetVersionResponse>(done2);
    server.Start("localhost:50051", 10);
  }
}
*/

template <class AS>
class Server {
  typedef AS AsyncService;
  AsyncService service_;
  std::unique_ptr<WataClientManager> cm_;
  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
  std::vector<std::unique_ptr<std::thread>> threads_;
  std::unique_ptr<grpc::Server> server_;

 public:
  GrpcServer(WataRoomManager* rm, WataActorManager* am) : rm_(rm), am_(am) {
    auto creds = grpc::GoogleDefaultCredentials();
    auto channel = grpc::CreateChannel("speech.googleapis.com", creds);
    cm_.reset(new WataClientManager(channel, 2));
  }

  ~GrpcServer() {
    //Shutdown();
  }

  void Start(std::string address, int threads) {
    grpc::ServerBuilder builder;
    // 認証せずに指定されたアドレスを listen する
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    // クライアントとやりとりするインスタンスとして service_ を登録する。
    // この場合、これは *非同期の* サービスに対応する。
    builder.RegisterService(&service_);

    // gRPC 実行時に非同期でやりとりするのに使われる completion queue を構築する
    for (int i = 0; i < threads; i++) {
      cqs_.push_back(builder.AddCompletionQueue());
    }

    // 最後にサーバを assemble する
    server_ = builder.BuildAndStart();
    SPDLOG_INFO("gRPC Server listening on {}", address);

    for (int i = 0; i < threads; i++) {
      auto cq = cqs_[i].get();
      threads_.push_back(std::unique_ptr<std::thread>(
          new std::thread([this, cq] { this->HandleRpcs(cq); })));
    }
  }

  void Shutdown() {
    server_->Shutdown();
    // サーバをシャットダウンした後に completion queue を削除する必要がある
    for (auto& cq : cqs_) {
      cq->Shutdown();
    }
    for (auto& thread : threads_) {
      thread->join();
    }
  }

 private:
  // 個々のリクエストを処理するための情報を持ったクラス
  class ConnectHandler {
    std::mutex mutex_;

    WataActorManager* am_;
    WataRoomManager* rm_;
    WataClientManager* cm_;
    Actor actor_;
    std::unique_ptr<ContinuousSpeechToText> stt_;
    std::deque<wata::SpeechData::Transcript> stt_transcripts_;

    // 非同期サーバの gRPC ランタイムでやりとりする
    wata::Room::AsyncService* service_;
    // 非同期サーバに通知するための producer-consumer キュー
    grpc::ServerCompletionQueue* cq_;
    // RPC のコンテキストで、圧縮・認証の利用や、メタデータのクライアントへの送信などを調整できる
    grpc::ServerContext ctx_;
    grpc::ServerAsyncReaderWriter<wata::StreamingResponse,
                                  wata::StreamingRequest>
        streamer_;

    wata::StreamingRequest request_;
    std::deque<std::shared_ptr<wata::StreamingResponse>> response_queue_;
    std::optional<int64_t> session_id_;

    struct AcceptorThunk : WataHandler {
      ConnectHandler* p;
      AcceptorThunk(ConnectHandler* p) : p(p) {}
      void Proceed(bool ok) override { p->ProceedToAccept(ok); }
    };
    AcceptorThunk acceptor_thunk_;
    friend class AcceptorThunk;

    struct ReaderThunk : WataHandler {
      ConnectHandler* p;
      ReaderThunk(ConnectHandler* p) : p(p) {}
      void Proceed(bool ok) override { p->ProceedToRead(ok); }
    };
    ReaderThunk reader_thunk_;
    friend class ReaderThunk;

    struct WriterThunk : WataHandler {
      ConnectHandler* p;
      WriterThunk(ConnectHandler* p) : p(p) {}
      void Proceed(bool ok) override { p->ProceedToWrite(ok); }
    };
    WriterThunk writer_thunk_;
    friend class WriterThunk;

    struct NotifierThunk : WataHandler {
      ConnectHandler* p;
      NotifierThunk(ConnectHandler* p) : p(p) {}
      void Proceed(bool ok) override { p->ProceedToNotify(ok); }
    };
    NotifierThunk notifier_thunk_;
    friend class NotifierThunk;

    // 状態マシン
    enum class ReadStatus { LISTENING, READING, FINISHED };
    enum class WriteStatus { LISTENING, WRITING, IDLE, FINISHING, FINISHED };
    ReadStatus read_status_ = ReadStatus::LISTENING;
    WriteStatus write_status_ = WriteStatus::LISTENING;

    void MaybeTerminate() {
      if (read_status_ == ReadStatus::FINISHED &&
          write_status_ == WriteStatus::FINISHED) {
        actor_.Terminate(am_);
      }
    }

    grpc::Alarm alarm_;
    void Notify() {
      alarm_.Set(cq_, gpr_time_0(GPR_TIMESPAN), &notifier_thunk_);
    }

   public:
    // gRPC ランタイムとの非同期通信に使われる "service" インスタンス（この場合は非同期サーバーを表す）と完了キュー "cq" を受け取る
    ConnectHandler(wata::Room::AsyncService* service,
                   grpc::ServerCompletionQueue* cq, WataRoomManager* rm,
                   WataActorManager* am, WataClientManager* cm, Actor actor)
        : service_(service),
          cq_(cq),
          rm_(rm),
          am_(am),
          cm_(cm),
          streamer_(&ctx_),
          actor_(actor),
          acceptor_thunk_(this),
          reader_thunk_(this),
          writer_thunk_(this),
          notifier_thunk_(this) {
      // リクエストを開始する
      service_->RequestConnect(&ctx_, &streamer_, cq_, cq_, &acceptor_thunk_);
    }

   private:
    void ProceedToAccept(bool ok) {
      SPDLOG_TRACE("ProceedToAccept: ok={}", ok);
      std::lock_guard<std::mutex> guard(mutex_);

      // 新しいリクエストが来たら、すぐに新しい ConnectHandler インスタンスを生成して
      // 次の新しいリクエストを受けれるようにする。
      auto actor = am_->PrepareActor();
      actor.Start(am_, "ConnectHandler",
                  new ConnectHandler(service_, cq_, rm_, am_, cm_, actor),
                  &ConnectHandler::HandleMessage,
                  &ConnectHandler::HandleTerminate);

      if (!ok) {
        SPDLOG_ERROR("accept failed");
        actor_.Terminate(am_);
        return;
      }

      read_status_ = ReadStatus::READING;
      streamer_.Read(&request_, &reader_thunk_);

      write_status_ = WriteStatus::IDLE;
    }

    void ProceedToRead(bool ok) {
      SPDLOG_TRACE("ProceedToRead: ok={}", ok);
      std::lock_guard<std::mutex> guard(mutex_);

      if (!ok) {
        // 読み込みがすべて完了した（あるいは失敗した）
        // あとは書き込み処理が終わるのを待つだけ

        actor_.Send(am_, WataActorMessage::CreateEmpty(
                             WataActorMessage::Type::ReceiveFromClient_Done));

        read_status_ = ReadStatus::FINISHED;
        MaybeTerminate();
        return;
      }

      actor_.Send(
          am_, WataActorMessage::CreateValue(
                   WataActorMessage::Type::ReceiveFromClient_StreamingRequest,
                   std::move(request_)));

      // 次の読み込み
      streamer_.Read(&request_, &reader_thunk_);
    }

    void ProceedToWrite(bool ok) {
      SPDLOG_TRACE("ProceedToWrite: ok={}", ok);
      std::lock_guard<std::mutex> guard(mutex_);

      if (!ok) {
        SPDLOG_ERROR("write failed");
        write_status_ = WriteStatus::FINISHED;
        MaybeTerminate();
        return;
      }

      if (write_status_ == WriteStatus::IDLE) {
      } else if (write_status_ == WriteStatus::WRITING) {
        response_queue_.pop_front();
        if (response_queue_.empty()) {
          write_status_ = WriteStatus::IDLE;
        } else {
          auto resp = response_queue_.front();
          if (resp) {
            // Response
            streamer_.Write(
                std::move(
                    *std::static_pointer_cast<wata::StreamingResponse>(resp)),
                &writer_thunk_);
          } else {
            // Finish
            streamer_.Finish(grpc::Status::OK, &writer_thunk_);
            write_status_ = WriteStatus::FINISHING;
          }
        }
      } else if (write_status_ == WriteStatus::FINISHING) {
        write_status_ = WriteStatus::FINISHED;
        MaybeTerminate();
      }
    }

    void ProceedToNotify(bool ok) {
      SPDLOG_TRACE("ProceedToNotify: ok={}", ok);
      std::lock_guard<std::mutex> guard(mutex_);

      if (!ok) {
        SPDLOG_WARN("Alarm cancelled");
        write_status_ = WriteStatus::FINISHED;
        MaybeTerminate();
        return;
      }
      if (response_queue_.empty()) {
        SPDLOG_WARN("response_queue_ is empty");
        write_status_ = WriteStatus::FINISHED;
        MaybeTerminate();
        return;
      }

      auto resp = response_queue_.front();
      if (resp) {
        // Response
        streamer_.Write(
            std::move(*std::static_pointer_cast<wata::StreamingResponse>(resp)),
            &writer_thunk_);
      } else {
        // Finish
        streamer_.Finish(grpc::Status::OK, &writer_thunk_);
        write_status_ = WriteStatus::FINISHING;
      }
    }

   public:
    ActorHandleResult HandleMessage(WataActorManager* am, Actor self,
                                    WataActorMessage message) {
      switch (message.type) {
        // クライアントへの送信
        case WataActorMessage::Type::SendToClient_StreamingResponse: {
          std::lock_guard<std::mutex> guard(mutex_);

          auto value = message.GetAs<wata::StreamingResponse>();
          assert(value);

          if (write_status_ == WriteStatus::IDLE) {
            write_status_ = WriteStatus::WRITING;
            response_queue_.push_back(value);
            Notify();
          } else if (write_status_ == WriteStatus::WRITING) {
            response_queue_.push_back(value);
          }
          break;
        }
        // クライアントへの送信完了
        case WataActorMessage::Type::SendToClient_Done: {
          std::lock_guard<std::mutex> guard(mutex_);

          if (write_status_ == WriteStatus::IDLE) {
            write_status_ = WriteStatus::FINISHING;
            streamer_.Finish(grpc::Status::OK, &writer_thunk_);
          } else if (write_status_ == WriteStatus::WRITING) {
            response_queue_.push_back(nullptr);
          }
          break;
        }
        // クライアントからの受信
        case WataActorMessage::Type::ReceiveFromClient_StreamingRequest: {
          // ルーム内の他のアクターへ Send しまくる
          auto value = message.GetAs<wata::StreamingRequest>();
          assert(value);

          // レスポンスはまとめて行う
          wata::StreamingResponse resp;

          // 全ての requests の処理
          for (int i = 0; i < value->requests_size(); i++) {
            wata::DataRequest& req = *value->mutable_requests(i);
            switch (req.request_case()) {
              // 入室
              case wata::DataRequest::kJoin: {
                // 接続済み
                if (session_id_) {
                  return ActorHandleResult::Complete;
                }
                auto session_id = req.join().session_id();
                auto room = rm_->GetOrCreateRoomBySessionID(session_id);
                room->JoinGrpc(session_id, self);
                session_id_ = session_id;

                break;
              }
              // 設定
              case wata::DataRequest::kConfig: {
                if (!session_id_) {
                  SPDLOG_ERROR("this connection not authorized");
                  return ActorHandleResult::Complete;
                }
                const auto& config = req.config();

                if (config.audio_sample_rate() != 8000 &&
                    config.audio_sample_rate() != 12000 &&
                    config.audio_sample_rate() != 16000 &&
                    config.audio_sample_rate() != 24000 &&
                    config.audio_sample_rate() != 48000) {
                  SPDLOG_ERROR("invalid sample rate: {}",
                               config.audio_sample_rate());
                  return ActorHandleResult::Complete;
                }

                if (config.enable_speech_to_text()) {
                  // Speech-to-Text 開始
                  stt_.reset(new ContinuousSpeechToText(
                      cm_, config.audio_sample_rate(),
                      [am, self](wata::SpeechData::Transcript ts) {
                        // 自分のアクターに送信
                        self.Send(am, WataActorMessage::CreateValue(
                                          WataActorMessage::Type::
                                              InternalCallback_SpeechToText,
                                          std::move(ts)));
                      }));
                }
                break;
              }
              // メッセージ送信
              case wata::DataRequest::kMessage: {
                if (!session_id_) {
                  SPDLOG_ERROR("this connection not authorized");
                  return ActorHandleResult::Complete;
                }

                wata::MessageRequest& message = *req.mutable_message();

                wata::MessageResponse* mr =
                    resp.add_responses()->mutable_message();
                mr->set_from_session_id(*session_id_);
                mr->set_allocated_name(message.release_name());
                mr->set_allocated_value(message.release_value());

                auto room = rm_->GetRoomBySessionID(*session_id_);
                if (room != nullptr) {
                  room->Send(std::move(resp));
                }

                break;
              }
              case wata::DataRequest::REQUEST_NOT_SET:
                break;
            }
          }

          // internal_data の処理モーション、音声、オーディオデータ送信
          if (value->has_internal_data()) {
            if (!session_id_) {
              // Join してない場合はエラー
              return ActorHandleResult::Complete;
            }

            wata::InternalDataRequest& data = *value->mutable_internal_data();

            // Speech-to-Text
            if (stt_) {
              const auto& ad = data.audio_data();
              for (int i = 0; i < ad.chunks_size(); i++) {
                const auto& chunk = ad.chunks(i);
                stt_->Write(chunk.data());
              }
            }

            wata::InternalDataResponse* dr = resp.mutable_internal_data();
            dr->set_from_session_id(*session_id_);
            dr->set_allocated_motion_data(data.release_motion_data());
            dr->set_allocated_video_data(data.release_video_data());
            dr->set_allocated_audio_data(data.release_audio_data());

            // 溜まってる Speech-to-Text のデータを設定
            if (!stt_transcripts_.empty()) {
              auto sd = dr->mutable_speech_data();
              for (auto& ts : stt_transcripts_) {
                auto tsdata = sd->add_transcripts();
                *tsdata = std::move(ts);
              }
              stt_transcripts_.clear();
            }
          }

          // データがあればクライアントへのレスポンスを返す
          if (resp.responses_size() != 0 || resp.has_internal_data()) {
            auto room = rm_->GetRoomBySessionID(*session_id_);
            if (room != nullptr) {
              room->Send(std::move(resp));
            }
          }
        }
        // クライアントからの受信完了
        case WataActorMessage::Type::ReceiveFromClient_Done: {
          // ルーム内の他のアクターへ Send しまくる
          break;
        }
        // Speech-to-Text
        case WataActorMessage::Type::InternalCallback_SpeechToText: {
          auto value = message.GetAs<wata::SpeechData::Transcript>();
          assert(value);

          // とりあえず溜め込んで、次のデータをクライアントに送信する時に一緒に送る
          stt_transcripts_.push_back(*value);

          break;
        }
        default:
          break;
      }

      return ActorHandleResult::Complete;
    }

    ActorHandleResult HandleTerminate(WataActorManager* am, Actor self,
                                      bool continuing) {
      return ActorHandleResult::Complete;
    }
  };

  void HandleRpcs(grpc::ServerCompletionQueue* cq) {
    Actor actor = am_->PrepareActor();
    actor.Start(am_, "ConnectHandler",
                new ConnectHandler(&service_, cq, rm_, am_, cm_.get(), actor),
                &ConnectHandler::HandleMessage,
                &ConnectHandler::HandleTerminate);

    void* got_tag = nullptr;
    bool ok = false;

    while (cq->Next(&got_tag, &ok)) {
      WataHandler* call = static_cast<WataHandler*>(got_tag);
      call->Proceed(ok);
    }
  }
};

}  // namespace ggrpc

#endif // GGRPC_SERVER_H_INCLUDED
