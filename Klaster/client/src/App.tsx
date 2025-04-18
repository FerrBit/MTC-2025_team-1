import React, { useEffect, useState, useCallback } from 'react';
import './App.css';
import './styles/WelcomeInfo.css'
import 'react-toastify/dist/ReactToastify.css';
import Header from './components/Header';
import AuthModal from './components/AuthModal';
import { ToastContainer, toast } from 'react-toastify';
import ClusteringDashboard from './components/ClusteringDashboard';

interface User {
    id: number;
    username: string;
    email: string;
}

type AuthFormData = {
  email: string;
  password: string;
  username?: string;
  confirm_password?: string;
};


const App = () => {
    const [isAuthenticated, setIsAuthenticated] = useState<boolean>(false);
    const [authToken, setAuthToken] = useState<string | null>(null);
    const [currentUser, setCurrentUser] = useState<User | null>(null);
    const [authLoading, setAuthLoading] = useState<boolean>(true);
    const [isRegisterOpen, setIsRegisterOpen] = useState(false);
    const [isLoginOpen, setIsLoginOpen] = useState(false);

    const handleLogout = useCallback(() => {
        localStorage.removeItem('authToken');
        setAuthToken(null);
        setCurrentUser(null);
        setIsAuthenticated(false);
        toast.info("Вы вышли из системы.");
    }, []);

    const fetchWithAuth = useCallback(async (url: string, options: RequestInit = {}) => {
        const headers = new Headers(options.headers || {});
        if (!(options.body instanceof FormData)) {
             headers.set('Content-Type', 'application/json');
        }

        if (authToken) {
            headers.set('Authorization', `Bearer ${authToken}`);
        }

        const finalOptions: RequestInit = {
            ...options,
            headers: headers
        };

        const response = await fetch(url, finalOptions);

        if (response.status === 401) {
            if (isAuthenticated) {
                 handleLogout();
                 toast.error('Сессия истекла или недействительна. Пожалуйста, войдите снова.');
            }
        }

        return response;
    }, [authToken, handleLogout, isAuthenticated]);

    useEffect(() => {
        const tokenFromStorage = localStorage.getItem('authToken');
        if (tokenFromStorage) {
            if (!authToken) {
                setAuthToken(tokenFromStorage);
            }
        } else {
            setAuthLoading(false);
        }
    }, [authToken]);


    useEffect(() => {
        if (authToken && !isAuthenticated) {
             if (!authLoading) { setAuthLoading(true); }

             fetchWithAuth('/api/me')
                .then(async response => {
                    if (response.status === 401) {
                        handleLogout();
                        return null;
                    }
                    if (!response.ok) {
                         const errorData = await response.json().catch(() => ({}));
                         throw new Error(errorData.error || `Ошибка проверки токена: ${response.status}`);
                    }
                    return response.json();
                })
                .then(data => {
                    if (data && data.user) {
                        setCurrentUser(data.user);
                        setIsAuthenticated(true);
                    } else if (data !== null) {
                         handleLogout();
                    }
                })
                .catch((err) => {
                     if (!(err.message && err.message.includes('Ошибка проверки токена: 401'))) {
                       console.error("Error validating token or fetching user data:", err);
                     }
                     handleLogout();
                })
                .finally(() => {
                    setAuthLoading(false);
                });
        } else if (!authToken && isAuthenticated) {
             handleLogout();
             setAuthLoading(false);
        } else if (!authToken && !isAuthenticated && authLoading) {
             setAuthLoading(false);
        }
    }, [authToken, isAuthenticated, fetchWithAuth, handleLogout, authLoading]);

    const handleRegister = async (formData: AuthFormData) => {
        const response = await fetch('/api/register', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(formData)
        });
        const data = await response.json();
        if (!response.ok) {
            throw new Error(data.error || 'Ошибка регистрации');
        }
        setIsRegisterOpen(false);
        toast.success(data.message || 'Регистрация успешна! Теперь вы можете войти.');
        setIsLoginOpen(true);
    };

    const handleLogin = async (formData: AuthFormData) => {
        const response = await fetch('/api/login', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(formData)
        });
        const data = await response.json();
        if (!response.ok) {
            throw new Error(data.error || 'Ошибка входа');
        }
        if (!data.access_token || !data.user) {
             throw new Error('Сервер не вернул токен или данные пользователя');
        }
        localStorage.setItem('authToken', data.access_token);
        setAuthToken(data.access_token);
        setCurrentUser(data.user);
        setIsAuthenticated(true);
        setIsLoginOpen(false);
        toast.success(`Добро пожаловать, ${data.user.username}!`);
    };

    const closeLoginModal = () => {
        setIsLoginOpen(false);
    };

    const closeRegisterModal = () => {
        setIsRegisterOpen(false);
    };

    if (authLoading && !isAuthenticated) {
        return <div style={{ textAlign: 'center', margin: '4rem 0', fontSize: '1.2em' }}>Проверка авторизации...</div>;
    }

    return (
      <div className="container">
          <Header
              isAuthenticated={isAuthenticated}
              user={currentUser}
              onLoginClick={() => setIsLoginOpen(true)}
              onRegisterClick={() => setIsRegisterOpen(true)}
              onLogoutClick={handleLogout}
          />

          {!isAuthenticated ? (
              <div className="welcome-info card">
                 <h3>Добро пожаловать в систему кластеризации изображений!</h3>
                 <p>Этот инструмент позволяет вам автоматически кластеризовать большие наборы изображений на основе их эмбеддингов и визуально оценивать результаты с помощью контактных отпечатков.</p>

                 <h4>Основные возможности:</h4>
                 <ul>
                    <li>Автоматическая кластеризация до 1,000,000 изображений.</li>
                    <li>Визуализация кластеров с помощью графиков и метрик.</li>
                    <li>Генерация "контактных отпечатков" - коллажей из изображений, ближайших к центру каждого кластера.</li>
                    <li>Возможность удаления нерелевантных кластеров (через удаление их отпечатков) с последующей автоматической рекластеризацией.</li>
                    <li>(Планируется) Инструменты для ручной корректировки кластеров.</li>
                 </ul>
                  <p style={{textAlign: 'center', marginTop: '1.5rem'}}>
                     Пожалуйста, <button className="link-like-btn" onClick={() => setIsLoginOpen(true)}>войдите</button> или <button className="link-like-btn" onClick={() => setIsRegisterOpen(true)}>зарегистрируйтесь</button>, чтобы начать работу.
                 </p>
              </div>
          ) : (
              <ClusteringDashboard fetchWithAuth={fetchWithAuth} />
          )}

            <AuthModal
                isOpen={isRegisterOpen}
                onClose={closeRegisterModal}
                onSubmit={handleRegister}
                title="Регистрация"
                submitButtonText="Зарегистрироваться"
            />

            <AuthModal
                isOpen={isLoginOpen}
                onClose={closeLoginModal}
                onSubmit={handleLogin}
                title="Вход"
                submitButtonText="Войти"
            />

            <ToastContainer
                position="bottom-right"
                autoClose={5000}
                hideProgressBar={false}
                newestOnTop={false}
                closeOnClick
                rtl={false}
                pauseOnFocusLoss
                draggable
                pauseOnHover
                theme="light"
            />
        </div>
    );
};

export default App;