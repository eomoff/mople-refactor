package com.codeit.mople.domain.user.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codeit.mople.domain.auth.repository.RefreshTokenRepository;
import com.codeit.mople.domain.auth.repository.SessionTokenRepository;
import com.codeit.mople.domain.user.dto.request.ChangePasswordRequest;
import com.codeit.mople.domain.user.dto.request.UserCreateRequest;
import com.codeit.mople.domain.user.dto.request.UserSearchRequest;
import com.codeit.mople.domain.user.dto.request.UserSortBy;
import com.codeit.mople.domain.user.dto.request.UserUpdateRequest;
import com.codeit.mople.domain.user.dto.response.UserDto;
import com.codeit.mople.domain.user.entity.User;
import com.codeit.mople.domain.user.event.UserSearchIndexEvent;
import com.codeit.mople.domain.user.exception.UserErrorCode;
import com.codeit.mople.domain.user.exception.UserException;
import com.codeit.mople.domain.user.repository.UserRepository;
import com.codeit.mople.domain.user.repository.search.UserSearchRepository;
import com.codeit.mople.global.config.KafkaProperties;
import com.codeit.mople.global.dto.CursorResponse;
import com.codeit.mople.global.dto.SearchResult;
import com.codeit.mople.global.dto.SortDirection;
import com.codeit.mople.global.outbox.OutboxService;
import com.codeit.mople.global.storage.FileStorageService;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@ExtendWith(MockitoExtension.class)
public class UserServiceTest {

  private static final String USER_SEARCH_INDEX_TOPIC = "mople.user-search-index-events.v1";

  @Mock
  private UserRepository userRepository;

  @Mock
  private PasswordEncoder passwordEncoder;

  @Mock
  private FileStorageService fileStorageService;

  @Mock
  private SessionTokenRepository sessionTokenRepository;

  @Mock
  private RefreshTokenRepository refreshTokenRepository;

  @Mock
  private UserSearchRepository searchRepository;

  @Mock
  private OutboxService outboxService;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private KafkaProperties kafkaProperties;

  @InjectMocks
  private UserService userService;

  private User user;

  @BeforeEach
  void setUp() {
    user = User.createUser("test@test.com", "encodedPassword", "testUser");
    TransactionSynchronizationManager.initSynchronization();
  }

  @AfterEach
  void tearDown() {
    TransactionSynchronizationManager.clear();
  }

  @Test
  @DisplayName("회원가입 성공")
  void signUp_success() {
    UUID userId = UUID.randomUUID();
    UserCreateRequest request = new UserCreateRequest("test@test.com", "rawPw123", "testUser");

    ReflectionTestUtils.setField(user, "id", userId);

    when(userRepository.existsByEmail(request.email())).thenReturn(false);
    when(passwordEncoder.encode(request.password())).thenReturn("encodedPw");
    when(userRepository.save(any(User.class))).thenReturn(user);
    when(kafkaProperties.topics().userSearchIndex()).thenReturn(USER_SEARCH_INDEX_TOPIC);

    UserDto response = userService.signUp(request);

    assertThat(response.email()).isEqualTo("test@test.com");

    verify(userRepository).save(any(User.class));
    verify(outboxService).save(
        any(),
        eq(USER_SEARCH_INDEX_TOPIC),
        eq(userId.toString()),
        argThat((UserSearchIndexEvent event) ->
            event.eventId() != null
                && event.userId().equals(userId)
                && event.email().equals(user.getEmail())
        )
    );
  }

  @Test
  @DisplayName("이메일 중복 시 예외 발생 및 중복된 이메일 정보 포함")
  void signUp_throwsException_whenEmailDuplicated() {
    UserCreateRequest request = new UserCreateRequest("dup@test.com", "rawPw123", "testUser");
    when(userRepository.existsByEmail(request.email())).thenReturn(true);

    assertThatThrownBy(() -> userService.signUp(request))
        .isInstanceOf(UserException.class)
        .hasFieldOrPropertyWithValue("errorCode", UserErrorCode.DUPLICATE_EMAIL)
        .satisfies(e -> {
          UserException ue = (UserException) e;
          assertThat(ue.getDetails()).containsEntry("email", "dup@test.com");
        });
  }

  @Test
  @DisplayName("대소문자만 다른 이메일로 가입을 시도하면 중복으로 처리됨")
  void signUp_throwsException_whenEmailDuplicatedWithDifferentCase() {
    UserCreateRequest request = new UserCreateRequest("Dup@Test.com", "rawPw123", "testUser");
    when(userRepository.existsByEmail("dup@test.com")).thenReturn(true);

    assertThatThrownBy(() -> userService.signUp(request))
        .isInstanceOf(UserException.class)
        .hasFieldOrPropertyWithValue("errorCode", UserErrorCode.DUPLICATE_EMAIL)
        .satisfies(e -> {
          UserException ue = (UserException) e;
          assertThat(ue.getDetails()).containsEntry("email", "dup@test.com");
        });
  }

  @Test
  @DisplayName("사용자 조회 성공")
  void getUser_success() {
    UUID userId = UUID.randomUUID();
    when(userRepository.findById(userId)).thenReturn(Optional.of(user));

    UserDto response = userService.getUser(userId);

    assertThat(response.email()).isEqualTo(user.getEmail());
  }

  @Test
  @DisplayName("존재하지 않는 사용자 조회 시 예외 발생")
  void getUser_throwsException_whenUserNotFound() {
    UUID userId = UUID.randomUUID();
    when(userRepository.findById(userId)).thenReturn(Optional.empty());

    assertThatThrownBy(() -> userService.getUser(userId))
        .isInstanceOf(UserException.class)
        .hasFieldOrPropertyWithValue("errorCode", UserErrorCode.USER_NOT_FOUND);
  }

  @Test
  @DisplayName("사용자 목록 조회 시 필터 조건 없이도 정상 동작함")
  void getUsers_success_withoutFilters() {
    UserSearchRequest request = new UserSearchRequest(
        null, null, null, null, null, 10,
        SortDirection.ASCENDING, UserSortBy.NAME
    );
    User user1 = User.createUser("user1@test.com", "encoded", "user1");
    when(userRepository.searchUsers(request, null)).thenReturn(List.of(user1));
    when(userRepository.countUsers(request, null)).thenReturn(1L);

    CursorResponse<UserDto> response = userService.getUsers(request);

    assertThat(response.data()).hasSize(1);
    assertThat(response.totalCount()).isEqualTo(1L);
    verify(userRepository).searchUsers(request, null);
  }

  @Test
  @DisplayName("사용자 목록 조회 시 totalCount는 실제 전체 개수를 반환함 (페이지 크기와 다름)")
  void getUsers_returnsActualTotalCount_notPageSize() {
    UserSearchRequest request = new UserSearchRequest(
        null, null, null, null, null, 2,
        SortDirection.ASCENDING, UserSortBy.NAME
    );
    User user1 = User.createUser("a@test.com", "encoded", "aa");
    User user2 = User.createUser("b@test.com", "encoded", "bb");
    User user3 = User.createUser("c@test.com", "encoded", "cc");
    when(userRepository.searchUsers(request, null)).thenReturn(List.of(user1, user2, user3));
    when(userRepository.countUsers(request, null)).thenReturn(100L);

    CursorResponse<UserDto> response = userService.getUsers(request);

    assertThat(response.data()).hasSize(2);
    assertThat(response.totalCount()).isEqualTo(100L);
  }

  @Test
  @DisplayName("사용자 목록 조회 시 hasNext와 nextCursor가 올바르게 설정됨")
  void getUsers_returnsCorrectCursorResponse() {
    UserSearchRequest request = new UserSearchRequest(
        null, null, null, null, null, 2,
        SortDirection.ASCENDING, UserSortBy.NAME
    );

    User user1 = User.createUser("a@test.com", "encoded", "aa");
    User user2 = User.createUser("b@test.com", "encoded", "bb");
    User user3 = User.createUser("c@test.com", "encoded", "cc");
    when(userRepository.searchUsers(request, null)).thenReturn(List.of(user1, user2, user3));
    when(userRepository.countUsers(request, null)).thenReturn(3L);

    CursorResponse<UserDto> response = userService.getUsers(request);

    assertThat(response.data()).hasSize(2);
    assertThat(response.hasNext()).isTrue();
    assertThat(response.nextCursor()).isEqualTo("bb");
    assertThat(response.sortBy()).isEqualTo("name");
    assertThat(response.totalCount()).isEqualTo(3L);
  }

  @Test
  @DisplayName("다음 페이지가 있으면 hasNext가 true이고 nextCursor/nextIdAfter가 채워짐")
  void getUsers_returnsHasNextTrue_whenMoreItemsExist() {
    UserSearchRequest request = new UserSearchRequest(
        null, null, null, null, null, 2,
        SortDirection.ASCENDING, UserSortBy.NAME
    );
    User user1 = User.createUser("user1@test.com", "encoded", "user1");
    User user2 = User.createUser("user2@test.com", "encoded", "user2");
    User user3 = User.createUser("user3@test.com", "encoded", "user3");
    when(userRepository.searchUsers(request, null)).thenReturn(List.of(user1, user2, user3));
    when(userRepository.countUsers(request, null)).thenReturn(3L);

    CursorResponse<UserDto> response = userService.getUsers(request);

    assertThat(response.data()).hasSize(2);
    assertThat(response.hasNext()).isTrue();
    assertThat(response.nextCursor()).isEqualTo("user2");
    assertThat(response.nextIdAfter()).isEqualTo(user2.getId());
  }

  @Test
  @DisplayName("마지막 페이지면 hasNext가 false이고 nextCursor는 null임")
  void getUsers_returnsHasNextFalse_whenLastPage() {
    UserSearchRequest request = new UserSearchRequest(
        null, null, null, null, null, 10,
        SortDirection.ASCENDING, UserSortBy.NAME
    );
    User user1 = User.createUser("user1@test.com", "encoded", "user1");
    when(userRepository.searchUsers(request, null)).thenReturn(List.of(user1));
    when(userRepository.countUsers(request, null)).thenReturn(1L);

    CursorResponse<UserDto> response = userService.getUsers(request);

    assertThat(response.hasNext()).isFalse();
    assertThat(response.nextCursor()).isNull();
    assertThat(response.nextIdAfter()).isNull();
  }

  @Test
  @DisplayName("sortBy가 email이면 nextCursor에 email 값이 사용됨")
  void getUsers_usesEmailAsCursor_whenSortByEmail() {
    UserSearchRequest request = new UserSearchRequest(
        null, null, null, null, null, 1,
        SortDirection.ASCENDING, UserSortBy.EMAIL
    );
    User user1 = User.createUser("user1@test.com", "encoded", "user1");
    User user2 = User.createUser("user2@test.com", "encoded", "user2");
    when(userRepository.searchUsers(request, null)).thenReturn(List.of(user1, user2));
    when(userRepository.countUsers(request, null)).thenReturn(2L);

    CursorResponse<UserDto> response = userService.getUsers(request);

    assertThat(response.nextCursor()).isEqualTo("user1@test.com");
  }

  @Test
  @DisplayName("이름만 변경")
  void updateProfile_success_nameOnly() {
    UUID userId = UUID.randomUUID();
    ReflectionTestUtils.setField(user, "id", userId);
    when(userRepository.findById(userId)).thenReturn(Optional.of(user));
    when(kafkaProperties.topics().userSearchIndex()).thenReturn(USER_SEARCH_INDEX_TOPIC);

    UserUpdateRequest request = new UserUpdateRequest("newName");

    UserDto response = userService.updateProfile(userId, request, null);

    assertThat(response.name()).isEqualTo("newName");
    verify(outboxService).save(
        any(),
        eq(USER_SEARCH_INDEX_TOPIC),
        eq(userId.toString()),
        argThat((UserSearchIndexEvent event) ->
            event.userId().equals(userId) && event.name().equals("newName"))
    );
  }

  @Test
  @DisplayName("이미지만 변경")
  void updateProfile_success_imageOnly() {
    UUID userId = UUID.randomUUID();
    ReflectionTestUtils.setField(user, "id", userId);
    when(userRepository.findById(userId)).thenReturn(Optional.of(user));
    when(fileStorageService.upload(any())).thenReturn("https://placeholder.mople.com/test.jpg");

    MockMultipartFile image = new MockMultipartFile("image", "test.jpg", "image/jpeg",
        "content".getBytes());
    UserUpdateRequest request = new UserUpdateRequest(null);

    UserDto response = userService.updateProfile(userId, request, image);

    assertThat(response.profileImageUrl()).isEqualTo("https://placeholder.mople.com/test.jpg");
    assertThat(response.name()).isEqualTo(user.getName());
  }

  @Test
  @DisplayName("이름과 이미지 둘 다 변경(기존 이미지가 있을 경우 삭제 호출 검증)")
  void updateProfile_success_both() {
    UUID userId = UUID.randomUUID();

    ReflectionTestUtils.setField(user, "profileImageUrl", "https://placeholder.mople.com/old.jpg");

    ReflectionTestUtils.setField(user, "id", userId);
    when(userRepository.findById(userId)).thenReturn(Optional.of(user));
    when(fileStorageService.upload(any())).thenReturn("https://placeholder.mople.com/test.jpg");

    MockMultipartFile image = new MockMultipartFile("image", "test.jpg", "image/jpeg",
        "content".getBytes());
    UserUpdateRequest request = new UserUpdateRequest("newName");

    UserDto response = userService.updateProfile(userId, request, image);

    assertThat(response.name()).isEqualTo("newName");
    assertThat(response.profileImageUrl()).isEqualTo("https://placeholder.mople.com/test.jpg");

    //트랜잭션 커밋 이벤트 강제 트리거
    TransactionSynchronizationManager.getSynchronizations()
        .forEach(TransactionSynchronization::afterCommit);

    //S3에서 기존 이미지를 삭제하는 로직이 정상 호출되었는지 검증
    verify(fileStorageService).delete("https://placeholder.mople.com/old.jpg");
  }

  @Test
  @DisplayName("비밀번호 변경 성공 시 기존 Refresh Token이 무효화됨")
  void changePassword_success() {
    UUID userId = UUID.randomUUID();
    when(userRepository.findById(userId)).thenReturn(Optional.of(user));
    when(passwordEncoder.encode("newPw123")).thenReturn("encodedNewPw");

    ChangePasswordRequest request = new ChangePasswordRequest("newPw123");

    userService.changePassword(userId, request);

    assertThat(user.getPassword()).isEqualTo("encodedNewPw");
    verify(refreshTokenRepository).invalidate(userId);
    verify(sessionTokenRepository).invalidate(userId);
  }

  @Test
  @DisplayName("사용자 이메일 검색 성공")
  void getUsers_success_withEmailLike() {
    UserSearchRequest request = new UserSearchRequest(
        "test", null, null, null, null, 10,
        SortDirection.ASCENDING, UserSortBy.EMAIL
    );

    UUID userId = UUID.randomUUID();
    User user = User.createUser("test@test.com", "encoded", "testUser");
    ReflectionTestUtils.setField(user, "id", userId);

    SearchResult searchResult = new SearchResult(
        List.of(userId),
        "test@test.com",
        null,
        false,
        1L
    );

    when(searchRepository.findAllByEmailContainingIgnoreCase(
        request.emailLike(),
        request.idAfter(),
        null,
        request.limitOrDefault(),
        request.sortByOrDefault(),
        request.sortDirectionOrDefault(),
        request.roleEqual(),
        request.isLocked()
    )).thenReturn(searchResult);

    when(userRepository.searchUsers(request, List.of(userId)))
        .thenReturn(List.of(user));

    CursorResponse<UserDto> response = userService.getUsers(request);

    assertThat(response.data()).hasSize(1);
    assertThat(response.data().get(0).email()).isEqualTo("test@test.com");
    assertThat(response.totalCount()).isEqualTo(1L);
    assertThat(response.hasNext()).isFalse();

    verify(searchRepository).findAllByEmailContainingIgnoreCase(
        request.emailLike(),
        request.idAfter(),
        null,
        request.limitOrDefault(),
        request.sortByOrDefault(),
        request.sortDirectionOrDefault(),
        request.roleEqual(),
        request.isLocked()
    );
  }

  @Test
  @DisplayName("ES 검색 결과 중 DB에 존재하지 않는 사용자는 제외됨")
  void getUsers_filtersOutUsersNotFoundInDb() {
    UserSearchRequest request = new UserSearchRequest(
        "test", null, null, null, null, 10,
        SortDirection.ASCENDING, UserSortBy.EMAIL
    );

    UUID existingUserId = UUID.randomUUID();
    UUID missingUserId = UUID.randomUUID();

    User user = User.createUser("test@test.com", "encoded", "testUser");
    ReflectionTestUtils.setField(user, "id", existingUserId);

    SearchResult searchResult = new SearchResult(
        List.of(existingUserId, missingUserId),
        "missing@test.com",
        missingUserId,
        true,
        2L
    );

    when(searchRepository.findAllByEmailContainingIgnoreCase(
        request.emailLike(),
        request.idAfter(),
        null,
        request.limitOrDefault(),
        request.sortByOrDefault(),
        request.sortDirectionOrDefault(),
        request.roleEqual(),
        request.isLocked()
    )).thenReturn(searchResult);

    when(userRepository.searchUsers(request, List.of(existingUserId, missingUserId)))
        .thenReturn(List.of(user));

    CursorResponse<UserDto> response = userService.getUsers(request);

    assertThat(response.data()).hasSize(1);
    assertThat(response.data().get(0).id()).isEqualTo(existingUserId);
    assertThat(response.hasNext()).isTrue();
    assertThat(response.nextCursor()).isEqualTo("missing@test.com");
    assertThat(response.nextIdAfter()).isEqualTo(missingUserId);
    assertThat(response.totalCount()).isEqualTo(2L);
  }

  @Test
  @DisplayName("빈 커서 입력 시 INVALID_CURSOR 예외 발생")
  void getUsers_throwsException_whenCursorIsBlank() {
    UserSearchRequest request = new UserSearchRequest(
        null, null, null, "", null, 10,
        SortDirection.ASCENDING, UserSortBy.NAME
    );

    assertThatThrownBy(() -> userService.getUsers(request))
        .isInstanceOf(UserException.class)
        .hasFieldOrPropertyWithValue(
            "errorCode",
            UserErrorCode.INVALID_CURSOR
        );
  }

  @Test
  @DisplayName("CREATED_AT 커서가 정상적으로 파싱됨")
  void getUsers_success_withCreatedAtCursor() {
    String cursor = "2026-08-24T10:00:00Z";

    UserSearchRequest request = new UserSearchRequest(
        "test", null, null, cursor, null, 10,
        SortDirection.ASCENDING, UserSortBy.CREATED_AT
    );

    UUID userId = UUID.randomUUID();
    User user = User.createUser("test@test.com", "encoded", "testUser");
    ReflectionTestUtils.setField(user, "id", userId);

    SearchResult searchResult = new SearchResult(
        List.of(userId),
        null,
        null,
        false,
        1L
    );

    when(searchRepository.findAllByEmailContainingIgnoreCase(
        request.emailLike(),
        request.idAfter(),
        Instant.parse(cursor),
        request.limitOrDefault(),
        request.sortByOrDefault(),
        request.sortDirectionOrDefault(),
        request.roleEqual(),
        request.isLocked()
    )).thenReturn(searchResult);

    when(userRepository.searchUsers(request, List.of(userId)))
        .thenReturn(List.of(user));

    CursorResponse<UserDto> response = userService.getUsers(request);

    assertThat(response.data()).hasSize(1);

    verify(searchRepository).findAllByEmailContainingIgnoreCase(
        request.emailLike(),
        request.idAfter(),
        Instant.parse(cursor),
        request.limitOrDefault(),
        request.sortByOrDefault(),
        request.sortDirectionOrDefault(),
        request.roleEqual(),
        request.isLocked()
    );
  }

  @Test
  @DisplayName("잘못된 CREATED_AT 커서 입력 시 INVALID_CURSOR 예외 발생")
  void getUsers_throwsException_whenCreatedAtCursorIsInvalid() {
    UserSearchRequest request = new UserSearchRequest(
        "test", null, null, "invalid-date", null, 10,
        SortDirection.ASCENDING, UserSortBy.CREATED_AT
    );

    assertThatThrownBy(() -> userService.getUsers(request))
        .isInstanceOf(UserException.class)
        .hasFieldOrPropertyWithValue(
            "errorCode",
            UserErrorCode.INVALID_CURSOR
        );
  }

  @Test
  @DisplayName("IS_LOCKED 커서가 true로 정상 파싱됨")
  void getUsers_success_withLockedTrueCursor() {
    UserSearchRequest request = new UserSearchRequest(
        "test", null, null, "true", null, 10,
        SortDirection.ASCENDING, UserSortBy.IS_LOCKED
    );

    UUID userId = UUID.randomUUID();
    User user = User.createUser("test@test.com", "encoded", "testUser");
    ReflectionTestUtils.setField(user, "id", userId);

    SearchResult searchResult = new SearchResult(
        List.of(userId),
        null,
        null,
        false,
        1L
    );

    when(searchRepository.findAllByEmailContainingIgnoreCase(
        request.emailLike(),
        request.idAfter(),
        true,
        request.limitOrDefault(),
        request.sortByOrDefault(),
        request.sortDirectionOrDefault(),
        request.roleEqual(),
        request.isLocked()
    )).thenReturn(searchResult);

    when(userRepository.searchUsers(request, List.of(userId)))
        .thenReturn(List.of(user));

    CursorResponse<UserDto> response = userService.getUsers(request);

    assertThat(response.data()).hasSize(1);

    verify(searchRepository).findAllByEmailContainingIgnoreCase(
        request.emailLike(),
        request.idAfter(),
        true,
        request.limitOrDefault(),
        request.sortByOrDefault(),
        request.sortDirectionOrDefault(),
        request.roleEqual(),
        request.isLocked()
    );
  }

  @Test
  @DisplayName("IS_LOCKED 커서가 false로 정상 파싱됨")
  void getUsers_success_withLockedFalseCursor() {
    UserSearchRequest request = new UserSearchRequest(
        "test", null, null, "false", null, 10,
        SortDirection.ASCENDING, UserSortBy.IS_LOCKED
    );

    UUID userId = UUID.randomUUID();
    User user = User.createUser("test@test.com", "encoded", "testUser");
    ReflectionTestUtils.setField(user, "id", userId);

    SearchResult searchResult = new SearchResult(
        List.of(userId),
        null,
        null,
        false,
        1L
    );

    when(searchRepository.findAllByEmailContainingIgnoreCase(
        request.emailLike(),
        request.idAfter(),
        false,
        request.limitOrDefault(),
        request.sortByOrDefault(),
        request.sortDirectionOrDefault(),
        request.roleEqual(),
        request.isLocked()
    )).thenReturn(searchResult);

    when(userRepository.searchUsers(request, List.of(userId)))
        .thenReturn(List.of(user));

    CursorResponse<UserDto> response = userService.getUsers(request);

    assertThat(response.data()).hasSize(1);
  }

  @Test
  @DisplayName("잘못된 IS_LOCKED 커서 입력 시 INVALID_CURSOR 예외 발생")
  void getUsers_throwsException_whenLockedCursorIsInvalid() {
    UserSearchRequest request = new UserSearchRequest(
        "test", null, null, "invalid", null, 10,
        SortDirection.ASCENDING, UserSortBy.IS_LOCKED
    );

    assertThatThrownBy(() -> userService.getUsers(request))
        .isInstanceOf(UserException.class)
        .hasFieldOrPropertyWithValue(
            "errorCode",
            UserErrorCode.INVALID_CURSOR
        );
  }

  @Test
  @DisplayName("ROLE 커서가 문자열로 정상 처리됨")
  void getUsers_success_withRoleCursor() {
    UserSearchRequest request = new UserSearchRequest(
        "test", null, null, "USER", null, 10,
        SortDirection.ASCENDING, UserSortBy.ROLE
    );

    UUID userId = UUID.randomUUID();
    User user = User.createUser("test@test.com", "encoded", "testUser");
    ReflectionTestUtils.setField(user, "id", userId);

    SearchResult searchResult = new SearchResult(
        List.of(userId),
        null,
        null,
        false,
        1L
    );

    when(searchRepository.findAllByEmailContainingIgnoreCase(
        request.emailLike(),
        request.idAfter(),
        "USER",
        request.limitOrDefault(),
        request.sortByOrDefault(),
        request.sortDirectionOrDefault(),
        request.roleEqual(),
        request.isLocked()
    )).thenReturn(searchResult);

    when(userRepository.searchUsers(request, List.of(userId)))
        .thenReturn(List.of(user));

    CursorResponse<UserDto> response = userService.getUsers(request);

    assertThat(response.data()).hasSize(1);

    verify(searchRepository).findAllByEmailContainingIgnoreCase(
        request.emailLike(),
        request.idAfter(),
        "USER",
        request.limitOrDefault(),
        request.sortByOrDefault(),
        request.sortDirectionOrDefault(),
        request.roleEqual(),
        request.isLocked()
    );
  }

  @Test
  @DisplayName("빈 MultipartFile이면 이미지 업로드를 수행하지 않음")
  void updateProfile_doesNotUpload_whenImageIsEmpty() {
    UUID userId = UUID.randomUUID();
    ReflectionTestUtils.setField(user, "id", userId);
    when(userRepository.findById(userId)).thenReturn(Optional.of(user));

    MockMultipartFile emptyImage = new MockMultipartFile(
        "image",
        "empty.jpg",
        "image/jpeg",
        new byte[0]
    );

    UserUpdateRequest request = new UserUpdateRequest("newName");

    UserDto response = userService.updateProfile(
        userId,
        request,
        emptyImage
    );

    assertThat(response.name()).isEqualTo("newName");

    verify(fileStorageService, org.mockito.Mockito.never()).upload(any());
  }

  @Test
  @DisplayName("기존 이미지가 없을 때 새 이미지만 업로드하고 기존 이미지 삭제는 수행하지 않음")
  void updateProfile_success_imageOnly_withoutOldImage() {
    UUID userId = UUID.randomUUID();

    ReflectionTestUtils.setField(user, "profileImageUrl", null);

    ReflectionTestUtils.setField(user, "id", userId);
    when(userRepository.findById(userId)).thenReturn(Optional.of(user));
    when(fileStorageService.upload(any()))
        .thenReturn("https://placeholder.mople.com/new.jpg");

    MockMultipartFile image = new MockMultipartFile(
        "image",
        "test.jpg",
        "image/jpeg",
        "content".getBytes()
    );

    UserUpdateRequest request = new UserUpdateRequest(null);

    UserDto response = userService.updateProfile(
        userId,
        request,
        image
    );

    assertThat(response.profileImageUrl())
        .isEqualTo("https://placeholder.mople.com/new.jpg");

    verify(fileStorageService).upload(image);
    verify(fileStorageService, org.mockito.Mockito.never())
        .delete(any());
  }
}
